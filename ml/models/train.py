"""
Module 6 — LSTM RUL Prediction
Sub-modules: Model Training Pipeline, Validation & Evaluation, Model
Serialization, Model Versioning.

Trains ONE combined LSTM (per feature config: raw or engineered) across
all four C-MAPSS sub-datasets, then evaluates separately on each
sub-dataset's held-out test set — matching how RUL results are reported
in the C-MAPSS literature even though training data is pooled.

Raw training retains its historical combined/raw paths and validates array
shape and values; the legacy NPZs do not independently prove feature order.
Engineered training requires a verified Module 05 bundle and a new explicit
output directory outside the bundle and existing ML artifact directories.
Historical engineered checkpoints are not certified by this input validation.

Usage:
    python -m ml.models.train --config raw
    python -m ml.models.train --config engineered --dataset-bundle /path/to/bundle --artifact-dir /path/to/new/output
    python -m ml.models.train --config raw --epochs 50 --batch-size 256
"""

from __future__ import annotations

import argparse
import copy
import json
import time
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import torch
from torch.utils.data import DataLoader

from ml.build_combined_dataset import COMBINED_DIR
from ml.config import CHECKPOINT_DIR, ML_ROOT, RANDOM_SEED, SUB_DATASETS, WINDOW_SIZE
from ml.models.compatibility import FEATURE_COLUMNS
from ml.models.dataset import WindowedNpzDataset
from ml.models.lstm import LSTMConfig, RULLSTM

torch.manual_seed(RANDOM_SEED)


def rmse_mae(preds: np.ndarray, targets: np.ndarray) -> tuple[float, float]:
    rmse = float(np.sqrt(np.mean((preds - targets) ** 2)))
    mae = float(np.mean(np.abs(preds - targets)))
    return rmse, mae


def evaluate(model: RULLSTM, loader: DataLoader, device: str) -> tuple[float, float]:
    model.eval()
    all_preds, all_targets = [], []
    with torch.no_grad():
        for X, y in loader:
            X = X.to(device)
            preds = model(X).cpu().numpy()
            all_preds.append(preds)
            all_targets.append(y.numpy())
    preds = np.concatenate(all_preds)
    targets = np.concatenate(all_targets)
    return rmse_mae(preds, targets)


def train(
    config_name: str,
    epochs: int = 30,
    batch_size: int = 128,
    lr: float = 1e-3,
    hidden_size: int = 64,
    num_layers: int = 2,
    dropout: float = 0.2,
    patience: int = 6,
    *,
    dataset_bundle: str | Path | None = None,
    artifact_dir: str | Path | None = None,
) -> dict:
    device = "cuda" if torch.cuda.is_available() else "cpu"
    if config_name == "raw":
        if dataset_bundle is not None or artifact_dir is not None:
            raise ValueError("raw training uses the legacy dataset and checkpoint paths")
        data_dir = COMBINED_DIR / "raw"
        expected_features = len(FEATURE_COLUMNS)
        ckpt_dir = CHECKPOINT_DIR / "raw"
        data_identity = {"kind": "legacy_raw", "dataset_path": str(data_dir),
                         "feature_order": None, "feature_contract_sha256": None,
                         "preprocessing_identity_sha256": None}
    elif config_name == "engineered":
        if dataset_bundle is None or artifact_dir is None:
            raise ValueError("engineered training requires --dataset-bundle and a new --artifact-dir")
        # The complete verified bundle, not an individual NPZ or a feature count,
        # is the authority for feature order and upstream provenance.
        from ml.features.dataset_generation import verify_generated_dataset
        from ml.features.feature_engineering import load_feature_contract

        bundle = Path(dataset_bundle).resolve()
        manifest = verify_generated_dataset(bundle)
        contract = load_feature_contract(bundle / "feature_contract.json")
        if (manifest["contract_sha256"] != contract.contract_hash
                or manifest["feature_order"] != list(contract.feature_order)
                or manifest["preprocessing_identity_sha256"] != contract.payload["preprocessing_artifact_identity"]):
            raise ValueError("engineered dataset contract mismatch")
        expected_features = len(contract.feature_order)
        data_dir = bundle / "engineered"
        data_identity = {"kind": "verified_engineered_bundle", "dataset_path": str(bundle),
                         "manifest_sha256": manifest["manifest_sha256"],
                         "feature_order": list(contract.feature_order),
                         "feature_contract_sha256": contract.contract_hash,
                         "preprocessing_identity_sha256": manifest["preprocessing_identity_sha256"]}
        destination = Path(artifact_dir)
        ckpt_dir = destination.resolve()
        if (destination.exists() or destination.is_symlink()
                or ckpt_dir.is_relative_to(ML_ROOT.resolve())
                or any(part.casefold() in {'saved', 'saved_v1_pre_leakage_fix', 'saved_v2_a1'} for part in ckpt_dir.parts)
                or ckpt_dir.is_relative_to(bundle)
                or bundle.is_relative_to(ckpt_dir)):
            raise ValueError("engineered artifact directory must be new and outside ML artifacts and the dataset bundle")
    else:
        raise ValueError("config_name must be raw or engineered")

    train_ds = WindowedNpzDataset(data_dir / "train.npz", expected_features=expected_features)
    val_ds = WindowedNpzDataset(data_dir / "val.npz", expected_features=expected_features)
    # Check all held-out files before model construction or any training.
    # Do not retain all four tensors in memory for the duration of training.
    for fd in SUB_DATASETS:
        test_path = data_dir / f"test_{fd}.npz"
        if not test_path.is_file():
            raise FileNotFoundError(f"Missing test dataset {fd}: {test_path}")
        WindowedNpzDataset(test_path, expected_features=expected_features)
    input_size = train_ds.X.shape[-1]

    train_generator = torch.Generator()
    train_generator.manual_seed(RANDOM_SEED)
    train_loader = DataLoader(
        train_ds, batch_size=batch_size, shuffle=True, generator=train_generator
    )
    val_loader = DataLoader(val_ds, batch_size=batch_size, shuffle=False)

    model_config = LSTMConfig(
        input_size=input_size, hidden_size=hidden_size,
        num_layers=num_layers, dropout=dropout,
    )
    model = RULLSTM(model_config).to(device)
    optimizer = torch.optim.Adam(model.parameters(), lr=lr)
    scheduler = torch.optim.lr_scheduler.ReduceLROnPlateau(
        optimizer, mode="min", factor=0.5, patience=3
    )
    loss_fn = torch.nn.MSELoss()

    best_val_rmse = float("inf")
    best_epoch = None
    best_state = None
    best_optimizer_state = None
    best_scheduler_state = None
    epochs_without_improvement = 0
    history = []

    print(f"\n=== Training LSTM [{config_name}] — input_size={input_size}, "
          f"device={device}, train windows={len(train_ds)}, val windows={len(val_ds)} ===")

    start_time = time.time()
    for epoch in range(1, epochs + 1):
        model.train()
        running_loss = 0.0
        for X, y in train_loader:
            X, y = X.to(device), y.to(device)
            optimizer.zero_grad()
            preds = model(X)
            loss = loss_fn(preds, y)
            loss.backward()
            optimizer.step()
            running_loss += loss.item() * len(y)
        train_loss = running_loss / len(train_ds)

        val_rmse, val_mae = evaluate(model, val_loader, device)
        scheduler.step(val_rmse)
        history.append({"epoch": epoch, "train_mse": train_loss, "val_rmse": val_rmse, "val_mae": val_mae})
        print(f"  Epoch {epoch:3d}/{epochs} | train MSE: {train_loss:8.3f} | "
              f"val RMSE: {val_rmse:7.3f} | val MAE: {val_mae:7.3f}")

        if val_rmse < best_val_rmse - 1e-3:
            best_val_rmse = val_rmse
            best_epoch = epoch
            best_state = {k: v.cpu().clone() for k, v in model.state_dict().items()}
            best_optimizer_state = copy.deepcopy(optimizer.state_dict())
            best_scheduler_state = copy.deepcopy(scheduler.state_dict())
            epochs_without_improvement = 0
        else:
            epochs_without_improvement += 1
            if epochs_without_improvement >= patience:
                print(f"  Early stopping at epoch {epoch} (no improvement for {patience} epochs)")
                break

    training_seconds = time.time() - start_time
    if best_state is None:
        raise ValueError("Training did not produce a finite validation-selected checkpoint")
    model.load_state_dict(best_state)

    training_config = {"epochs_requested": epochs, "epochs_run": len(history),
                       "batch_size": batch_size, "lr": lr, "patience": patience,
                       "seed": RANDOM_SEED, "optimizer": "Adam", "loss": "MSELoss"}
    checkpoint_metadata = {"input_feature_count": input_size, "window_size": WINDOW_SIZE,
                           "best_epoch": best_epoch, "best_val_rmse": best_val_rmse,
                           "training_config": training_config, "data_identity": data_identity,
                           "resume_supported": False}

    # --- Model Serialization + Model Versioning ---
    version_tag = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    ckpt_dir.mkdir(parents=True, exist_ok=(config_name == "raw"))
    ckpt_path = ckpt_dir / f"lstm_{config_name}_{version_tag}.pt"
    torch.save(
        {
            "model_state": best_state,
            "config": model_config.to_dict(),
            "optimizer_state": best_optimizer_state,
            "scheduler_state": best_scheduler_state,
            "epoch": best_epoch,
            "best_val_rmse": best_val_rmse,
            "resume_supported": False,
            "metadata": checkpoint_metadata,
        },
        ckpt_path,
    )

    # --- Validation & Evaluation: per-sub-dataset test RMSE/MAE ---
    per_fd_results = {}
    for fd in SUB_DATASETS:
        test_path = data_dir / f"test_{fd}.npz"
        test_ds = WindowedNpzDataset(test_path, expected_features=expected_features)
        test_loader = DataLoader(test_ds, batch_size=batch_size, shuffle=False)
        rmse, mae = evaluate(model, test_loader, device)
        per_fd_results[fd] = {"rmse": rmse, "mae": mae, "n_test_windows": len(test_ds)}
        print(f"  Test [{fd}]: RMSE={rmse:.3f}, MAE={mae:.3f} (n={len(test_ds)})")

    metadata = {
        "version": version_tag,
        "config_name": config_name,
        "model_config": model_config.to_dict(),
        "hyperparameters": {
            "epochs_run": len(history), "batch_size": batch_size, "lr": lr, "patience": patience,
        },
        "best_val_rmse": best_val_rmse,
        "best_epoch": best_epoch,
        "checkpoint_metadata": checkpoint_metadata,
        "training_seconds": round(training_seconds, 1),
        "per_fd_test_results": per_fd_results,
        "checkpoint_path": str(ckpt_path),
        "history": history,
    }
    meta_path = ckpt_dir / f"lstm_{config_name}_{version_tag}_metadata.json"
    with open(meta_path, "w") as f:
        json.dump(metadata, f, indent=2)
    print(f"\n  Saved checkpoint: {ckpt_path}")
    print(f"  Saved metadata:   {meta_path}")

    return metadata


def main() -> None:
    parser = argparse.ArgumentParser(description="Train the combined LSTM RUL model")
    parser.add_argument("--config", choices=["raw", "engineered"], required=True)
    parser.add_argument("--epochs", type=int, default=30)
    parser.add_argument("--batch-size", type=int, default=128)
    parser.add_argument("--lr", type=float, default=1e-3)
    parser.add_argument("--hidden-size", type=int, default=64)
    parser.add_argument("--num-layers", type=int, default=2)
    parser.add_argument("--dropout", type=float, default=0.2)
    parser.add_argument("--patience", type=int, default=6)
    parser.add_argument("--dataset-bundle", type=Path, help="verified Module 05 bundle root (engineered only)")
    parser.add_argument("--artifact-dir", type=Path, help="new output directory outside the bundle and ML tree (engineered only)")
    args = parser.parse_args()

    train(
        config_name=args.config, epochs=args.epochs, batch_size=args.batch_size,
        lr=args.lr, hidden_size=args.hidden_size, num_layers=args.num_layers,
        dropout=args.dropout, patience=args.patience,
        dataset_bundle=args.dataset_bundle, artifact_dir=args.artifact_dir,
    )


if __name__ == "__main__":
    main()
