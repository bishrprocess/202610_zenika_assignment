"""
Silver layer, combine raw Bronze CSVs into single dataset.
"""

from __future__ import annotations

import logging
from pathlib import Path

import polars as pl

from src.config import Config, load_config

logger = logging.getLogger(__name__)

def read_raw_files(raw_path: Path) -> dict[str, pl.DataFrame]:
    """
    Read raw csv files into polars dataframe and log filename, row count and column count.
    :param raw_path: Path to raw Bronze CSVs
    :return: dict[str, pl.DataFrame] containing [filename, CSVs read into polars dataframe]
    """
    files = sorted(raw_path.glob("*.csv"))
    if not files:
        raise FileNotFoundError("No csv files found in {raw_path}".format(raw_path=raw_path))

    frames: dict[str, pl.DataFrame] = {}
    for file in files:
        df = pl.read_csv(file, infer_schema=False)
        frames[file.name] = df.with_columns(
            pl.lit(file.name).alias("source_file")
        )
        logger.info("Read %s: %d rows, %d cols", file.name, df.height, df.width)
    return frames


def find_authoritative_reference_frame(
        cfg: Config,
        frames: dict[str, pl.DataFrame],
        reference_month: str
) -> tuple[str, pl.Dataframe]:
    """
    Find and return a tuple containing the filename and dataframe of the file that contains the authoritative reference
    data segment.
    :param cfg: Config object to extract relevant variables
    :param frames: dict[str, pl.DataFrame] containing [filename, CSVs read into dataframe]
    :param reference_month:
    :return:
    """
    #
    month_col = None
    data_matches = [
        name for name, df in frames.items()
            if month_col in df.columns and (df[month_col] == reference_month).any()
    ]

if __name__ == "__main__":
    cfg = load_config()

    raw_path = Path(cfg.path("bronze_raw"))
    print(raw_path)

    frames = read_raw_files(raw_path)

    print(frames.keys())