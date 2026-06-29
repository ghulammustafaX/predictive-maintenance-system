"""
Module 6 — LSTM RUL Prediction
Sub-modules: Model Training Pipeline, Validation & Evaluation, Model
Serialization, Model Versioning.

Trains ONE combined LSTM (per feature config: raw or engineered) across
all four C-MAPSS sub-datasets, then evaluates separately on each
sub-dataset's held-out test set — matching how RUL results are reported
in the C-MAPSS literature even though training data is pooled.

Usage:
    python -m ml.models.train --config raw
    python -m ml.models.train --config engineered
    python -m ml.models.train --config raw --epochs 50 --batch-size 256
"""

from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timezone

import numpy as np
import torch
from torch.utils.data import DataLoader

from ml.build_combined_dataset import COMBINED_DIR
from ml.config import CHECKPOINT_DIR, RANDOM_SEED, SUB_DATASETS
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
) -> dict:
    device = "cuda" if torch.cuda.is_available() else "cpu"
    data_dir = COMBINED_DIR / config_name

    train_ds = WindowedNpzDataset(data_dir / "train.npz")
    val_ds = WindowedNpzDataset(data_dir / "val.npz")
    input_size = train_ds.X.shape[-1]

    train_loader = DataLoader(train_ds, batch_size=batch_size, shuffle=True)
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
    best_state = None
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
            best_state = {k: v.cpu().clone() for k, v in model.state_dict().items()}
            epochs_without_improvement = 0
        else:
            epochs_without_improvement += 1
            if epochs_without_improvement >= patience:
                print(f"  Early stopping at epoch {epoch} (no improvement for {patience} epochs)")
                break

    training_seconds = time.time() - start_time
    model.load_state_dict(best_state)

    # --- Model Serialization + Model Versioning ---
    version_tag = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    ckpt_dir = CHECKPOINT_DIR / config_name
    ckpt_dir.mkdir(parents=True, exist_ok=True)
    ckpt_path = ckpt_dir / f"lstm_{config_name}_{version_tag}.pt"
    torch.save({"model_state": best_state, "config": model_config.to_dict()}, ckpt_path)

    # --- Validation & Evaluation: per-sub-dataset test RMSE/MAE ---
    per_fd_results = {}
    for fd in SUB_DATASETS:
        test_path = data_dir / f"test_{fd}.npz"
        if not test_path.exists():
            continue
        test_ds = WindowedNpzDataset(test_path)
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
    args = parser.parse_args()

    train(
        config_name=args.config, epochs=args.epochs, batch_size=args.batch_size,
        lr=args.lr, hidden_size=args.hidden_size, num_layers=args.num_layers,
        dropout=args.dropout, patience=args.patience,
    )


if __name__ == "__main__":
    main()
