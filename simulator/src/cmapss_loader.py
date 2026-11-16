"""
NASA C-MAPSS Turbofan Dataset Loader & Kafka/MQTT Streamer  —  Module 1

Responsibilities
----------------
- Load train, test, and RUL ground-truth files for FD001-FD004.
- Assign piecewise-linear RUL labels to training records.
- Associate RUL ground-truth with the last observed test cycle per engine.
- Stream each row as a JSON telemetry event with a full metadata envelope.
- Publish via the reusable KafkaProducerClient (direct Kafka mode) or
  via the MQTTPublisher (MQTT mode, routed to Kafka by the bridge).
- Support single replay and continuous replay with configurable limits.
- Handle SIGTERM and KeyboardInterrupt gracefully.

Environment variables
---------------------
KAFKA_BROKER            Kafka bootstrap server              (default: kafka:9092)
SENSOR_TOPIC            Kafka topic for C-MAPSS events      (default: sensor-telemetry)
DATASET_ID              FD001 | FD002 | FD003 | FD004       (default: FD001)
SOURCE_SPLIT            train | test                         (default: train)
STREAM_INTERVAL         Seconds between rows                 (default: 0.01)
REPLAY_MODE             single | continuous                  (default: single)
MAX_REPLAY_CYCLES       Max number of full replays (0=unlimited, default: 0)
REPLAY_PAUSE_SECONDS    Pause between replays                (default: 1.0)
RUN_ID                  Fixed run ID; auto-generated if unset
PUBLISH_TRANSPORT       kafka | mqtt | both                  (default: kafka)
MQTT_BROKER             MQTT broker host                     (default: mosquitto)
MQTT_PORT               MQTT broker port                     (default: 1883)
MQTT_TOPIC_CMAPSS       MQTT topic for C-MAPSS               (default: pms/telemetry/cmapss)
MQTT_QOS                MQTT QoS level (0|1|2)               (default: 1)
RUL_CAP                 Piecewise-linear RUL ceiling         (default: 125)
DATA_PATH               Override dataset file path
"""

from __future__ import annotations

import logging
import os
import signal
import sys
import time
from pathlib import Path
from typing import Iterator

import numpy as np
import pandas as pd
from dotenv import load_dotenv

from event_schema import SOURCE_CMAPSS, build_envelope, new_run_id

load_dotenv()

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
SENSOR_TOPIC = os.getenv("SENSOR_TOPIC", "sensor-telemetry")
DATASET_ID = os.getenv("DATASET_ID", "FD001").upper()
SOURCE_SPLIT = os.getenv("SOURCE_SPLIT", "train").lower()
STREAM_INTERVAL = float(os.getenv("STREAM_INTERVAL", "0.01"))
REPLAY_MODE = os.getenv("REPLAY_MODE", "single").lower()
MAX_REPLAY_CYCLES = int(os.getenv("MAX_REPLAY_CYCLES", "0"))
REPLAY_PAUSE_SECONDS = float(os.getenv("REPLAY_PAUSE_SECONDS", "1.0"))
PUBLISH_TRANSPORT = os.getenv("PUBLISH_TRANSPORT", "kafka").lower()
MQTT_BROKER = os.getenv("MQTT_BROKER", "mosquitto")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_TOPIC_CMAPSS = os.getenv("MQTT_TOPIC_CMAPSS", "pms/telemetry/cmapss")
MQTT_QOS = int(os.getenv("MQTT_QOS", "1"))
RUL_CAP = int(os.getenv("RUL_CAP", "125"))

SUPPORTED_DATASETS = {"FD001", "FD002", "FD003", "FD004"}
SUPPORTED_SPLITS = {"train", "test"}

# Column schema — matches ml/config.py
COLUMNS = (
    ["unit_id", "time_cycles"]
    + [f"operational_setting_{i}" for i in range(1, 4)]
    + [f"sensor_{i}" for i in range(1, 22)]
)


# ---------------------------------------------------------------------------
# Dataset loading
# ---------------------------------------------------------------------------

def _resolve_data_path(dataset_id: str, split: str) -> Path:
    """Resolve dataset file path from env override or conventional location."""
    override = os.getenv("DATA_PATH")
    if override:
        return Path(override)
    base = Path(os.getenv("CMAPSS_DATA_DIR", "/app/data/cmapss"))
    return base / f"{split}_{dataset_id}.txt"


def load_cmapss(dataset_id: str, split: str) -> pd.DataFrame:
    """Load a C-MAPSS text file into a typed DataFrame.

    Validates the dataset_id and split before reading.  Drops any trailing
    all-NaN columns that some whitespace-delimited files produce.

    Raises
    ------
    ValueError        — unknown dataset_id or split
    FileNotFoundError — data file not present
    """
    dataset_id = dataset_id.upper()
    split = split.lower()
    if dataset_id not in SUPPORTED_DATASETS:
        raise ValueError(f"Unsupported dataset_id '{dataset_id}'. Expected one of {SUPPORTED_DATASETS}")
    if split not in SUPPORTED_SPLITS:
        raise ValueError(f"Unsupported split '{split}'. Expected 'train' or 'test'")

    path = _resolve_data_path(dataset_id, split)
    if not path.exists():
        raise FileNotFoundError(f"C-MAPSS dataset not found: {path}")

    logger.info("Loading C-MAPSS %s/%s from %s", dataset_id, split, path)
    df = pd.read_csv(str(path), sep=r"\s+", header=None, engine="python")
    # Drop trailing all-NaN columns (trailing whitespace artifact)
    df = df.loc[:, ~df.isna().all()]
    if len(df.columns) < len(COLUMNS):
        raise ValueError(
            f"Expected at least {len(COLUMNS)} columns, got {len(df.columns)} in {path}"
        )
    df = df.iloc[:, : len(COLUMNS)]
    df.columns = COLUMNS

    # Cast to appropriate types
    df["unit_id"] = df["unit_id"].astype(int)
    df["time_cycles"] = df["time_cycles"].astype(int)

    # Validate no genuine NaN in core numeric columns
    nan_count = df[COLUMNS[2:]].isna().sum().sum()
    if nan_count > 0:
        logger.warning(
            "Dataset %s/%s contains %d NaN sensor values — will be passed as null",
            dataset_id,
            split,
            nan_count,
        )

    logger.info(
        "Loaded %d rows, %d engines (dataset=%s split=%s)",
        len(df),
        df["unit_id"].nunique(),
        dataset_id,
        split,
    )
    return df


def load_rul(dataset_id: str) -> pd.Series:
    """Load RUL ground-truth for the test split.

    Returns a Series indexed 1..N (unit_id) → RUL value.
    The RUL file has one value per test engine, in unit-ID order.
    """
    dataset_id = dataset_id.upper()
    if dataset_id not in SUPPORTED_DATASETS:
        raise ValueError(f"Unsupported dataset_id '{dataset_id}'")

    base = Path(os.getenv("CMAPSS_DATA_DIR", "/app/data/cmapss"))
    path = base / f"RUL_{dataset_id}.txt"
    if not path.exists():
        raise FileNotFoundError(f"RUL file not found: {path}")

    rul = pd.read_csv(str(path), sep=r"\s+", header=None, engine="python")
    rul.columns = ["rul"]
    rul.index = rul.index + 1   # unit IDs are 1-indexed
    rul.index.name = "unit_id"
    return rul["rul"]


def assign_rul_labels(df: pd.DataFrame, rul_cap: int = RUL_CAP) -> pd.DataFrame:
    """Add a 'rul' column to a training DataFrame using piecewise-linear targets.

    For each engine, the final observed cycle is treated as failure.
    RUL at any cycle = min(cycles_remaining, rul_cap).

    This is the standard approach for C-MAPSS training targets.
    """
    df = df.copy()
    max_cycles = df.groupby("unit_id")["time_cycles"].transform("max")
    raw_rul = max_cycles - df["time_cycles"]
    df["rul"] = raw_rul.clip(upper=rul_cap)
    return df


def attach_test_rul(df: pd.DataFrame, rul_ground_truth: pd.Series) -> pd.DataFrame:
    """Attach RUL reference to a test DataFrame.

    For each engine, the last observed cycle has rul = ground_truth[unit_id].
    Earlier cycles have rul = ground_truth[unit_id] + (last_cycle - cycle).
    The resulting 'rul' column is capped at RUL_CAP and is for reference only
    — it must not be fed into the model as an input feature.
    """
    df = df.copy()
    last_cycles = df.groupby("unit_id")["time_cycles"].transform("max")
    rul_at_last = df["unit_id"].map(rul_ground_truth)
    raw_rul = rul_at_last + (last_cycles - df["time_cycles"])
    df["rul"] = raw_rul.clip(upper=RUL_CAP)
    return df


# ---------------------------------------------------------------------------
# Row iterator
# ---------------------------------------------------------------------------

def iter_rows(
    df: pd.DataFrame,
    run_id: str,
    dataset_id: str,
    source_split: str,
    sequence_start: int = 0,
) -> Iterator[tuple[dict, str, int]]:
    """Yield (payload, kafka_key, sequence) for every row in df.

    The Kafka message key is ``{dataset_id}:{unit_id}:{run_id}`` so all
    messages for the same engine in the same run land on the same partition.
    Sequence numbers are global across all engines in one replay pass.
    """
    seq = sequence_start
    for _, row in df.iterrows():
        seq += 1
        unit_id = int(row["unit_id"])
        asset_id = f"engine_{unit_id}"

        envelope = build_envelope(
            run_id=run_id,
            dataset_id=dataset_id,
            asset_id=asset_id,
            sequence=seq,
            source_type=SOURCE_CMAPSS,
            source_split=source_split,
        )

        # Sensor payload: replace NaN with None for safe JSON serialisation
        sensor_data = {
            k: (None if pd.isna(v) else float(v))
            for k, v in row.items()
        }
        # Keep unit_id as int, time_cycles as int
        sensor_data["unit_id"] = unit_id
        sensor_data["time_cycles"] = int(row["time_cycles"])

        payload = {**envelope.to_dict(), **sensor_data}
        kafka_key = f"{dataset_id}:{unit_id}:{run_id}"
        yield payload, kafka_key, seq


# ---------------------------------------------------------------------------
# Simulation controller
# ---------------------------------------------------------------------------

class SimulationController:
    """Drive C-MAPSS streaming with configurable replay and graceful shutdown.

    Parameters mirror the module-level environment variables so the
    controller can be instantiated directly in tests without env setup.
    """

    def __init__(
        self,
        dataset_id: str = DATASET_ID,
        source_split: str = SOURCE_SPLIT,
        stream_interval: float = STREAM_INTERVAL,
        replay_mode: str = REPLAY_MODE,
        max_replay_cycles: int = MAX_REPLAY_CYCLES,
        replay_pause_seconds: float = REPLAY_PAUSE_SECONDS,
        publish_transport: str = PUBLISH_TRANSPORT,
        kafka_topic: str = SENSOR_TOPIC,
        kafka_broker: str = KAFKA_BROKER,
        mqtt_broker: str = MQTT_BROKER,
        mqtt_port: int = MQTT_PORT,
        mqtt_topic: str = MQTT_TOPIC_CMAPSS,
        mqtt_qos: int = MQTT_QOS,
        rul_cap: int = RUL_CAP,
        fixed_run_id: str | None = None,
    ) -> None:
        self.dataset_id = dataset_id.upper()
        self.source_split = source_split.lower()
        self.stream_interval = stream_interval
        self.replay_mode = replay_mode.lower()
        self.max_replay_cycles = max_replay_cycles
        self.replay_pause_seconds = replay_pause_seconds
        self.publish_transport = publish_transport.lower()
        self.kafka_topic = kafka_topic
        self.kafka_broker = kafka_broker
        self.mqtt_broker = mqtt_broker
        self.mqtt_port = mqtt_port
        self.mqtt_topic = mqtt_topic
        self.mqtt_qos = mqtt_qos
        self.rul_cap = rul_cap
        self._fixed_run_id = fixed_run_id
        self._stop = False
        self._kafka_producer = None
        self._mqtt_client = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def _handle_signal(self, signum: int, _frame: object) -> None:
        logger.info("Signal %d received — stopping after current row", signum)
        self._stop = True

    def _connect_kafka(self) -> None:
        from kafka_producer import KafkaProducerClient
        self._kafka_producer = KafkaProducerClient(bootstrap_servers=self.kafka_broker)
        self._kafka_producer.connect()

    def _connect_mqtt(self) -> None:
        try:
            from mqtt_publisher import MQTTPublisher
            self._mqtt_client = MQTTPublisher(
                broker=self.mqtt_broker,
                port=self.mqtt_port,
                default_qos=self.mqtt_qos,
            )
            self._mqtt_client.connect()
        except ImportError:
            logger.error("mqtt_publisher module not available — MQTT disabled")
            self._mqtt_client = None

    def _publish(self, payload: dict, kafka_key: str) -> None:
        """Publish to configured transport(s)."""
        if self.publish_transport in ("kafka", "both") and self._kafka_producer:
            self._kafka_producer.send(
                self.kafka_topic, payload, key=kafka_key
            )
        if self.publish_transport in ("mqtt", "both") and self._mqtt_client:
            self._mqtt_client.publish(self.mqtt_topic, payload)

    def _shutdown(self) -> None:
        if self._kafka_producer:
            self._kafka_producer.close()
            self._kafka_producer = None
        if self._mqtt_client:
            self._mqtt_client.disconnect()
            self._mqtt_client = None

    # ------------------------------------------------------------------
    # Data preparation
    # ------------------------------------------------------------------

    def _load(self) -> pd.DataFrame:
        df = load_cmapss(self.dataset_id, self.source_split)
        if self.source_split == "train":
            df = assign_rul_labels(df, self.rul_cap)
        else:
            try:
                rul_gt = load_rul(self.dataset_id)
                df = attach_test_rul(df, rul_gt)
            except FileNotFoundError:
                logger.warning("RUL ground-truth file not found — skipping RUL attachment")
        return df

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    def run(self) -> None:
        """Start the simulation.  Blocks until completion or stop signal."""
        signal.signal(signal.SIGTERM, self._handle_signal)

        logger.info(
            "SimulationController starting: dataset=%s split=%s interval=%.3fs "
            "mode=%s max_replays=%d transport=%s",
            self.dataset_id,
            self.source_split,
            self.stream_interval,
            self.replay_mode,
            self.max_replay_cycles,
            self.publish_transport,
        )

        # Connect producers
        if self.publish_transport in ("kafka", "both"):
            self._connect_kafka()
        if self.publish_transport in ("mqtt", "both"):
            self._connect_mqtt()

        # Load data once — avoids repeated I/O across replays
        df = self._load()

        replay_count = 0
        global_sequence = 0

        try:
            while not self._stop:
                run_id = self._fixed_run_id or new_run_id()
                replay_count += 1
                logger.info(
                    "--- Replay %d | run_id=%s | dataset=%s | rows=%d ---",
                    replay_count,
                    run_id,
                    self.dataset_id,
                    len(df),
                )

                rows_sent = 0
                for payload, kafka_key, seq in iter_rows(
                    df,
                    run_id=run_id,
                    dataset_id=self.dataset_id,
                    source_split=self.source_split,
                    sequence_start=global_sequence,
                ):
                    if self._stop:
                        break

                    self._publish(payload, kafka_key)
                    rows_sent += 1
                    global_sequence = seq

                    if rows_sent % 1000 == 0:
                        logger.info(
                            "Replay %d | sent %d rows | unit=%s | cycle=%d",
                            replay_count,
                            rows_sent,
                            payload.get("asset_id"),
                            payload.get("time_cycles", 0),
                        )

                    time.sleep(self.stream_interval)

                logger.info(
                    "Replay %d complete — sent %d rows (run_id=%s)",
                    replay_count,
                    rows_sent,
                    run_id,
                )

                # Decide whether to continue
                if self.replay_mode != "continuous":
                    logger.info("Replay mode is 'single' — exiting")
                    break

                if self.max_replay_cycles > 0 and replay_count >= self.max_replay_cycles:
                    logger.info(
                        "Reached max_replay_cycles=%d — exiting",
                        self.max_replay_cycles,
                    )
                    break

                if not self._stop:
                    logger.info(
                        "Pausing %.1f s before replay %d…",
                        self.replay_pause_seconds,
                        replay_count + 1,
                    )
                    time.sleep(self.replay_pause_seconds)

        except KeyboardInterrupt:
            logger.info("KeyboardInterrupt — stopping simulation")
        finally:
            self._shutdown()
            logger.info(
                "SimulationController stopped after %d replay(s), "
                "%d total events published",
                replay_count,
                global_sequence,
            )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    print("=" * 60)
    print("NASA C-MAPSS TURBOFAN SIMULATOR")
    print("Predictive Maintenance System — Module 1")
    print("=" * 60)

    if DATASET_ID not in SUPPORTED_DATASETS:
        logger.error("Invalid DATASET_ID '%s'. Must be one of %s", DATASET_ID, SUPPORTED_DATASETS)
        sys.exit(1)

    if SOURCE_SPLIT not in SUPPORTED_SPLITS:
        logger.error("Invalid SOURCE_SPLIT '%s'. Must be 'train' or 'test'", SOURCE_SPLIT)
        sys.exit(1)

    controller = SimulationController(
        fixed_run_id=os.getenv("RUN_ID") or None,
    )
    controller.run()


if __name__ == "__main__":
    main()
