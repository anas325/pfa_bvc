"""
Common setup for all figure generation scripts.
Sets matplotlib style and defines the output directory.

Import pattern in other scripts:
    import importlib.util, sys, pathlib
    spec = importlib.util.spec_from_file_location(
        "_setup", pathlib.Path(__file__).parent / "00_setup.py")
    _s = importlib.util.module_from_spec(spec); spec.loader.exec_module(_s)
    from _setup import *        # re-export names into current namespace
"""

import os
import sys
from pathlib import Path

import matplotlib
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
import pandas as pd

matplotlib.rcParams.update({
    "font.family": "DejaVu Sans",
    "font.size": 11,
    "axes.titlesize": 13,
    "axes.labelsize": 11,
    "axes.spines.top": False,
    "axes.spines.right": False,
    "axes.grid": True,
    "grid.alpha": 0.35,
    "figure.dpi": 150,
    "savefig.dpi": 150,
    "savefig.bbox": "tight",
    "savefig.facecolor": "white",
})

FIGURES_DIR = Path(__file__).parent.parent / "figures"
FIGURES_DIR.mkdir(parents=True, exist_ok=True)

# Palettes
PALETTE_SENTIMENT = {
    "positive": "#2ecc71",
    "neutral": "#95a5a6",
    "negative": "#e74c3c",
}

PALETTE_STRATEGIES = {
    "S1 - Classificateur": "#2980b9",
    "S2 - Régresseur": "#8e44ad",
    "S3 - Top-3": "#27ae60",
    "MASI (Benchmark)": "#e67e22",
}

# Data paths
NOTEBOOKS_DATA = Path(__file__).parent.parent.parent / "Notebooks" / "signal_pipeline" / "data"
PIPELINES_DATA = Path(__file__).parent.parent.parent / "Pipelines" / "data"


def save(fig: plt.Figure, name: str) -> None:
    path = FIGURES_DIR / name
    fig.savefig(path)
    plt.close(fig)
    print(f"Saved: {path}")
