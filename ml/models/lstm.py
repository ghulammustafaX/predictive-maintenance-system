"""
Module 6 — LSTM RUL Prediction
Sub-module: LSTM Architecture

Multi-layer LSTM with configurable hidden units, dropout regularization,
and a fully connected output head for RUL regression.
"""

from __future__ import annotations

from dataclasses import dataclass, asdict

import torch
import torch.nn as nn


@dataclass
class LSTMConfig:
    input_size: int
    hidden_size: int = 64
    num_layers: int = 2
    dropout: float = 0.2

    def to_dict(self) -> dict:
        return asdict(self)


class RULLSTM(nn.Module):
    """Predicts a single scalar RUL value from a (window_size,
    input_size) sequence of sensor/feature readings."""

    def __init__(self, config: LSTMConfig):
        super().__init__()
        self.config = config
        self.lstm = nn.LSTM(
            input_size=config.input_size,
            hidden_size=config.hidden_size,
            num_layers=config.num_layers,
            batch_first=True,
            dropout=config.dropout if config.num_layers > 1 else 0.0,
        )
        self.head = nn.Sequential(
            nn.Linear(config.hidden_size, config.hidden_size // 2),
            nn.ReLU(),
            nn.Dropout(config.dropout),
            nn.Linear(config.hidden_size // 2, 1),
        )

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        # x: (batch, window_size, input_size)
        _, (h_n, _) = self.lstm(x)
        last_hidden = h_n[-1]  # final layer's hidden state: (batch, hidden_size)
        return self.head(last_hidden).squeeze(-1)  # (batch,)
