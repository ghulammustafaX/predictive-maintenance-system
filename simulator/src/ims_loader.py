"""
IMS Bearing Dataset Loader & Feature Extractor  —  Module 1

Supports all three IMS test archives:
  1st_test.rar  — 2156 snapshot files, 8 channels (4 bearings × 2 channels each)
  2nd_test.rar  — 984  snapshot files, 4 channels (4 bearings × 1 channel each)
  3rd_test.rar  — 6324 snapshot files, 4 channels (4 bearings × 1 channel each)
                  (stored internally as 4th_test/txt/…)

Each file is a tab-separated ASCII matrix of float32 vibration readings:
  Rows    = 20 480  (one second of acquisition at 20 480 Hz)
  Columns = 4 or 8  (channel per bearing)

File names encode the acquisition timestamp in the format YYYY.MM.DD.HH.MM.SS.

The loader can:
  - Discover all snapshot files inside a RAR archive without full extraction.
  - Extract one snapshot at a time (stream processing, bounded memory).
  - Parse and validate the vibration matrix.
  - Reconstruct acquisition timestamps from filenames.
  - Extract time-domain and frequency-domain features per bearing channel.
  - Yield event payloads with the standard PMS metadata envelope.

Environment variables
---------------------
IMS_DATA_DIR        Directory containing the .rar files (default: /app/data/ims)
IMS_CHUNK_SIZE      Rows to read per feature window (default: 20480 = full snapshot)
IMS_STREAM_INTERVAL Seconds to sleep between snapshot events (default: 0.0)
UNRAR_TOOL          Path to UnRAR executable (default: unrar)
"""

from __future__ import annotations

import logging
import os
import subprocess
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

import numpy as np
import pandas as pd

from event_schema import SOURCE_IMS, build_envelope

logger = logging.getLogger(__name__)

IMS_DATA_DIR = Path(os.getenv("IMS_DATA_DIR", "/app/data/ims"))
IMS_CHUNK_SIZE = int(os.getenv("IMS_CHUNK_SIZE", "20480"))
UNRAR_TOOL = os.getenv("UNRAR_TOOL", "unrar")

# ---------------------------------------------------------------------------
# Archive metadata
# ---------------------------------------------------------------------------

ARCHIVE_CONFIGS: dict[str, dict] = {
    "1st_test": {
        "archive": "1st_test.rar",
        "prefix": "1st_test/",
        "n_channels": 8,
        "bearings": {
            "bearing_1_ch1": 0, "bearing_1_ch2": 1,
            "bearing_2_ch1": 2, "bearing_2_ch2": 3,
            "bearing_3_ch1": 4, "bearing_3_ch2": 5,
            "bearing_4_ch1": 6, "bearing_4_ch2": 7,
        },
        "sampling_hz": 20480,
    },
    "2nd_test": {
        "archive": "2nd_test.rar",
        "prefix": "2nd_test/",
        "n_channels": 4,
        "bearings": {
            "bearing_1": 0,
            "bearing_2": 1,
            "bearing_3": 2,
            "bearing_4": 3,
        },
        "sampling_hz": 20480,
    },
    "3rd_test": {
        "archive": "3rd_test.rar",
        "prefix": "4th_test/txt/",   # RAR internal path differs from archive name
        "n_channels": 4,
        "bearings": {
            "bearing_1": 0,
            "bearing_2": 1,
            "bearing_3": 2,
            "bearing_4": 3,
        },
        "sampling_hz": 20480,
    },
}

TIMESTAMP_FMT = "%Y.%m.%d.%H.%M.%S"


# ---------------------------------------------------------------------------
# Archive discovery
# ---------------------------------------------------------------------------

def list_snapshots(test_name: str, data_dir: Path = IMS_DATA_DIR) -> list[str]:
    """Return sorted internal paths of all snapshot files in a RAR archive.

    Does not extract any data — reads only the RAR directory headers.

    Parameters
    ----------
    test_name : '1st_test' | '2nd_test' | '3rd_test'
    data_dir  : directory that holds the .rar files

    Returns
    -------
    List of internal archive paths, sorted chronologically by filename.
    """
    cfg = _get_config(test_name)
    rar_path = data_dir / cfg["archive"]
    if not rar_path.exists():
        raise FileNotFoundError(f"IMS archive not found: {rar_path}")

    try:
        import rarfile
        rarfile.UNRAR_TOOL = UNRAR_TOOL
        with rarfile.RarFile(str(rar_path)) as rf:
            names = rf.namelist()
    except Exception as exc:
        raise RuntimeError(f"Cannot list archive {rar_path}: {exc}") from exc

    prefix = cfg["prefix"]
    # Keep only snapshot files (not directories)
    snapshots = sorted(
        n for n in names
        if n.startswith(prefix) and not n.endswith("/") and _is_timestamp_name(n)
    )
    logger.info("Found %d snapshots in %s", len(snapshots), rar_path.name)
    return snapshots


def _is_timestamp_name(path: str) -> bool:
    name = path.rsplit("/", 1)[-1]
    try:
        datetime.strptime(name, TIMESTAMP_FMT)
        return True
    except ValueError:
        return False


def _get_config(test_name: str) -> dict:
    if test_name not in ARCHIVE_CONFIGS:
        raise ValueError(
            f"Unknown IMS test name '{test_name}'. "
            f"Expected one of {list(ARCHIVE_CONFIGS)}"
        )
    return ARCHIVE_CONFIGS[test_name]


# ---------------------------------------------------------------------------
# Single-snapshot extraction & parsing
# ---------------------------------------------------------------------------

def extract_snapshot(
    rar_path: Path,
    internal_path: str,
    n_channels: int,
    chunk_size: int = IMS_CHUNK_SIZE,
) -> np.ndarray:
    """Extract one IMS snapshot file and return a (rows × n_channels) float32 array.

    Uses a temporary directory for extraction; temp files are cleaned up
    regardless of success or failure.

    Parameters
    ----------
    rar_path      : Path to the .rar archive
    internal_path : Internal path of the snapshot within the archive
    n_channels    : Expected number of channels (4 or 8)
    chunk_size    : How many rows to read (20480 = full snapshot)

    Raises
    ------
    RuntimeError  : If extraction or parsing fails
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        # unrar-free 0.3.x ignores the destination path argument and always
        # extracts to the current working directory.  We chdir into the temp
        # directory first, then restore CWD afterwards.
        orig_cwd = os.getcwd()
        try:
            os.chdir(tmpdir)
            result = subprocess.run(
                [UNRAR_TOOL, "e", str(rar_path), internal_path, "-y", "-inul"],
                capture_output=True,
            )
        finally:
            os.chdir(orig_cwd)

        if result.returncode != 0:
            raise RuntimeError(
                f"UnRAR extraction failed (rc={result.returncode}): "
                f"{result.stderr.decode('utf-8', errors='replace')[:200]}"
            )

        fname = internal_path.rsplit("/", 1)[-1]
        extracted = Path(tmpdir) / fname
        if not extracted.exists():
            raise RuntimeError(
                f"Extracted file not found: {extracted}. "
                f"unrar-free may not support this archive format."
            )

        try:
            # IMS files are tab-separated ASCII with Windows CRLF line endings.
            arr = np.loadtxt(str(extracted), dtype=np.float32, max_rows=chunk_size)
        except Exception as exc:
            raise RuntimeError(
                f"Failed to parse snapshot {internal_path}: {exc}"
            ) from exc

    if arr.ndim == 1:
        arr = arr.reshape(-1, 1)

    if arr.shape[1] != n_channels:
        raise ValueError(
            f"Expected {n_channels} channels in {internal_path}, "
            f"got {arr.shape[1]}"
        )

    return arr


def parse_snapshot_timestamp(internal_path: str) -> datetime:
    """Parse acquisition timestamp from an IMS snapshot filename."""
    name = internal_path.rsplit("/", 1)[-1]
    try:
        return datetime.strptime(name, TIMESTAMP_FMT).replace(tzinfo=timezone.utc)
    except ValueError as exc:
        raise ValueError(
            f"Cannot parse timestamp from filename '{name}': {exc}"
        ) from exc


# ---------------------------------------------------------------------------
# Feature extraction
# ---------------------------------------------------------------------------

def extract_features(signal: np.ndarray, sampling_hz: int = 20480) -> dict[str, float]:
    """Compute time-domain and frequency-domain features for a 1-D vibration signal.

    Parameters
    ----------
    signal      : 1-D float32 array of vibration readings
    sampling_hz : Sampling frequency in Hz

    Returns
    -------
    Dictionary of feature name → float value.
    """
    if signal.ndim != 1:
        raise ValueError(f"Expected 1-D signal, got shape {signal.shape}")
    if len(signal) < 4:
        raise ValueError(f"Signal too short for feature extraction: {len(signal)} samples")

    n = len(signal)

    # Time domain
    rms = float(np.sqrt(np.mean(signal ** 2)))
    mean_val = float(np.mean(signal))
    std_val = float(np.std(signal))
    peak_pos = float(np.max(signal))
    peak_neg = float(np.min(signal))
    peak_to_peak = peak_pos - peak_neg

    # Avoid division by zero
    crest_factor = float(np.abs(peak_pos) / rms) if rms > 1e-12 else 0.0

    # Kurtosis: E[(x-mean)^4] / std^4
    if std_val > 1e-12:
        kurtosis = float(np.mean(((signal - mean_val) / std_val) ** 4))
    else:
        kurtosis = 0.0

    # Frequency domain — single-sided magnitude spectrum
    spectrum = np.abs(np.fft.rfft(signal))
    freqs = np.fft.rfftfreq(n, d=1.0 / sampling_hz)
    dominant_freq = float(freqs[np.argmax(spectrum[1:]) + 1])  # skip DC

    # Spectral kurtosis proxy: kurtosis of the spectrum magnitude
    if len(spectrum) >= 4 and np.std(spectrum) > 1e-12:
        spectral_kurtosis = float(
            np.mean(((spectrum - np.mean(spectrum)) / np.std(spectrum)) ** 4)
        )
    else:
        spectral_kurtosis = 0.0

    # Energy in bearing fault frequency bands (approximate for 20kHz / 20480 samples)
    # BPFO-like band: 2-5 kHz; BPFI-like band: 5-10 kHz
    freq_resolution = sampling_hz / n
    band_low_idx = int(2000 / freq_resolution)
    band_mid_idx = int(5000 / freq_resolution)
    band_high_idx = int(10000 / freq_resolution)
    band_low_idx = min(band_low_idx, len(spectrum) - 1)
    band_mid_idx = min(band_mid_idx, len(spectrum) - 1)
    band_high_idx = min(band_high_idx, len(spectrum) - 1)

    energy_bpfo_band = float(np.sum(spectrum[band_low_idx:band_mid_idx] ** 2))
    energy_bpfi_band = float(np.sum(spectrum[band_mid_idx:band_high_idx] ** 2))
    total_energy = float(np.sum(spectrum ** 2))

    return {
        "rms": rms,
        "mean": mean_val,
        "std": std_val,
        "peak_pos": peak_pos,
        "peak_neg": peak_neg,
        "peak_to_peak": peak_to_peak,
        "crest_factor": crest_factor,
        "kurtosis": kurtosis,
        "dominant_freq_hz": dominant_freq,
        "spectral_kurtosis": spectral_kurtosis,
        "energy_2k_5k_hz": energy_bpfo_band,
        "energy_5k_10k_hz": energy_bpfi_band,
        "total_energy": total_energy,
    }


def extract_all_features(
    snapshot: np.ndarray,
    bearing_names: list[str],
    sampling_hz: int = 20480,
) -> dict[str, dict[str, float]]:
    """Extract features for every bearing channel in a snapshot.

    Returns a dict keyed by bearing name.
    """
    if snapshot.shape[1] != len(bearing_names):
        raise ValueError(
            f"snapshot has {snapshot.shape[1]} columns but "
            f"{len(bearing_names)} bearing names were provided"
        )
    return {
        name: extract_features(snapshot[:, col_idx], sampling_hz)
        for col_idx, name in enumerate(bearing_names)
    }


# ---------------------------------------------------------------------------
# Event payload builder
# ---------------------------------------------------------------------------

def build_ims_payload(
    test_name: str,
    snapshot_path: str,
    snapshot: np.ndarray,
    run_id: str,
    sequence: int,
    bearing_name: str,
    channel_data: np.ndarray,
    features: dict[str, float],
    sampling_hz: int,
    chunk_size: int,
    include_raw: bool = False,
) -> dict:
    """Build a full telemetry event payload for one IMS bearing channel."""
    ts = parse_snapshot_timestamp(snapshot_path)
    asset_id = f"{test_name}_{bearing_name}"

    envelope = build_envelope(
        run_id=run_id,
        dataset_id=f"IMS_{test_name}",
        asset_id=asset_id,
        sequence=sequence,
        source_type=SOURCE_IMS,
        source_split="test",
    )

    payload: dict = {
        **envelope.to_dict(),
        # Measurement metadata
        "test_name": test_name,
        "snapshot_file": snapshot_path.rsplit("/", 1)[-1],
        "bearing_name": bearing_name,
        "acquisition_timestamp": ts.isoformat(),
        "timestamp_source": "filename",   # reconstructed, not instrument-recorded
        "sampling_hz": sampling_hz,
        "samples_per_snapshot": chunk_size,
        # Extracted features (safe for LSTM / anomaly detection)
        "features": features,
        "n_samples": len(channel_data),
    }

    if include_raw:
        # Truncate to first 512 samples for transport; full data stays on disk
        payload["raw_samples_512"] = channel_data[:512].tolist()

    return payload


# ---------------------------------------------------------------------------
# Streaming iterator
# ---------------------------------------------------------------------------

def iter_ims_events(
    test_name: str,
    run_id: str,
    data_dir: Path = IMS_DATA_DIR,
    chunk_size: int = IMS_CHUNK_SIZE,
    sequence_start: int = 0,
    include_raw: bool = False,
) -> Iterator[tuple[dict, str, int]]:
    """Yield (payload, kafka_key, sequence) for every bearing channel in every snapshot.

    For a snapshot with N bearing channels, N events are yielded (one per bearing).
    Events for the same snapshot share the same acquisition timestamp.

    Parameters
    ----------
    test_name      : '1st_test' | '2nd_test' | '3rd_test'
    run_id         : unique replay ID
    data_dir       : directory containing the .rar archives
    chunk_size     : rows per snapshot to read
    sequence_start : start sequence number
    include_raw    : include first 512 raw samples in payload (larger messages)
    """
    cfg = _get_config(test_name)
    rar_path = data_dir / cfg["archive"]
    n_channels = cfg["n_channels"]
    bearings = cfg["bearings"]
    bearing_names = list(bearings.keys())
    sampling_hz = cfg["sampling_hz"]

    snapshots = list_snapshots(test_name, data_dir)
    seq = sequence_start

    for snap_path in snapshots:
        try:
            snapshot = extract_snapshot(rar_path, snap_path, n_channels, chunk_size)
        except (RuntimeError, ValueError) as exc:
            logger.error("Skipping snapshot %s: %s", snap_path, exc)
            continue

        features_by_bearing = extract_all_features(snapshot, bearing_names, sampling_hz)

        for bearing_name, features in features_by_bearing.items():
            seq += 1
            channel_idx = bearings[bearing_name]
            channel_data = snapshot[:, channel_idx]
            kafka_key = f"IMS_{test_name}:{bearing_name}:{run_id}"

            payload = build_ims_payload(
                test_name=test_name,
                snapshot_path=snap_path,
                snapshot=snapshot,
                run_id=run_id,
                sequence=seq,
                bearing_name=bearing_name,
                channel_data=channel_data,
                features=features,
                sampling_hz=sampling_hz,
                chunk_size=chunk_size,
                include_raw=include_raw,
            )
            yield payload, kafka_key, seq


# ---------------------------------------------------------------------------
# IMS simulation controller
# ---------------------------------------------------------------------------

class IMSSimulationController:
    """Drive IMS streaming with configurable replay and graceful shutdown."""

    def __init__(
        self,
        test_name: str = os.getenv("IMS_TEST_NAME", "2nd_test"),
        stream_interval: float = float(os.getenv("IMS_STREAM_INTERVAL", "0.0")),
        replay_mode: str = os.getenv("REPLAY_MODE", "single"),
        kafka_topic: str = os.getenv("BEARING_TOPIC", "bearing-telemetry"),
        kafka_broker: str = os.getenv("KAFKA_BROKER", "kafka:9092"),
        mqtt_topic: str = os.getenv("MQTT_TOPIC_IMS", "pms/telemetry/ims"),
        publish_transport: str = os.getenv("PUBLISH_TRANSPORT", "kafka"),
        data_dir: Path = IMS_DATA_DIR,
        include_raw: bool = False,
    ) -> None:
        self.test_name = test_name
        self.stream_interval = stream_interval
        self.replay_mode = replay_mode.lower()
        self.kafka_topic = kafka_topic
        self.kafka_broker = kafka_broker
        self.mqtt_topic = mqtt_topic
        self.publish_transport = publish_transport.lower()
        self.data_dir = data_dir
        self.include_raw = include_raw
        self._stop = False
        self._kafka_producer = None
        self._mqtt_client = None

    def _connect_kafka(self) -> None:
        from kafka_producer import KafkaProducerClient
        self._kafka_producer = KafkaProducerClient(bootstrap_servers=self.kafka_broker)
        self._kafka_producer.connect()

    def _publish(self, payload: dict, kafka_key: str) -> None:
        if self.publish_transport in ("kafka", "both") and self._kafka_producer:
            self._kafka_producer.send(self.kafka_topic, payload, key=kafka_key)
        if self.publish_transport in ("mqtt", "both") and self._mqtt_client:
            self._mqtt_client.publish(self.mqtt_topic, payload)

    def _shutdown(self) -> None:
        if self._kafka_producer:
            self._kafka_producer.close()
            self._kafka_producer = None
        if self._mqtt_client:
            self._mqtt_client.disconnect()
            self._mqtt_client = None

    def run(self) -> None:
        """Stream IMS bearing events until completion or stop signal."""
        import signal as _signal
        _signal.signal(_signal.SIGTERM, lambda s, f: setattr(self, "_stop", True))

        logger.info(
            "IMSSimulationController starting: test=%s mode=%s transport=%s",
            self.test_name,
            self.replay_mode,
            self.publish_transport,
        )

        if self.publish_transport in ("kafka", "both"):
            self._connect_kafka()

        replay_count = 0
        global_seq = 0

        try:
            while not self._stop:
                from event_schema import new_run_id
                run_id = new_run_id()
                replay_count += 1
                events_sent = 0

                for payload, kafka_key, seq in iter_ims_events(
                    self.test_name,
                    run_id=run_id,
                    data_dir=self.data_dir,
                    sequence_start=global_seq,
                    include_raw=self.include_raw,
                ):
                    if self._stop:
                        break
                    self._publish(payload, kafka_key)
                    events_sent += 1
                    global_seq = seq
                    if events_sent % 100 == 0:
                        logger.info(
                            "IMS replay %d | sent %d events | bearing=%s",
                            replay_count,
                            events_sent,
                            payload.get("bearing_name"),
                        )
                    if self.stream_interval > 0:
                        import time
                        time.sleep(self.stream_interval)

                logger.info(
                    "IMS replay %d complete — %d events (run_id=%s)",
                    replay_count,
                    events_sent,
                    run_id,
                )

                if self.replay_mode != "continuous":
                    break

        except KeyboardInterrupt:
            logger.info("IMS simulation interrupted by user")
        finally:
            self._shutdown()
