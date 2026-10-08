"""
Silver layer, combine raw Bronze CSVs into single dataset.

As per requirements, there is an authoritative reference month that will be the strict schema
to be followed. This can be configured in the config.yaml file.
"""

from __future__ import annotations

import logging
from pathlib import Path

import polars as pl

from src.config import Config, load_config

logger = logging.getLogger(__name__)

SOURCE_COL = "source_file"

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
            pl.lit(file.name).alias(SOURCE_COL)
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
    :return: Returns a tuple containing the filename and dataframe of the file that contains the authoritative reference
    However, it will log a warning in case multiple matches are found and return the first match identified.
    """
    # Get data month col as configured
    month_col = cfg.require("scope.data_month_col")

    # Check whether in given frame dictionary matching data to reference month exist (can be multiple frames - datasets)
    data_matches = [
        name for name, df in frames.items()
            if month_col in df.columns and (df[month_col] == reference_month).any()
    ]

    if not data_matches:
        raise ValueError ("No raw file contains reference month {ref_month}".format(ref_month=reference_month))

    if len(data_matches) > 1:
        logger.warning(
            "Reference month %s found in multiple files %s: using %s", reference_month, data_matches,
            data_matches[0]
        )

    return data_matches[0], frames[data_matches[0]]


def combine_frames(
        cfg: Config,
        frames: dict[str, pl.DataFrame],
) -> pl.Dataframe:
    """
    Combine raw Bronze CSVs into single dataset with the reference frame defining the authoritative schema.
    :param cfg: Config object to extract relevant variables
    :param frames: dict[str, pl.DataFrame] containing [filename, CSVs read into dataframe]
    :return: Polars dataframe containing combined CSVs with only columns by the determined authoritative schema.
    """
    reference_month = cfg.require("reference.authoritative_dataset_month")
    reference_filename, reference_df = find_authoritative_reference_frame(
        cfg=cfg,
        frames=frames,
        reference_month=reference_month
    )

    reference_cols = [c for c in reference_df.columns if c != SOURCE_COL]
    logger.info("Reference filename: %s: authoritative columns: %s", reference_filename, reference_cols)

    # Diagonal union merge of datasets - fills nulls where data isn't available across datasets
    combined_df = pl.concat(list(frames.values()), how="diagonal")

    extra_cols = [c for c in combined_df.columns if c not in reference_cols + [SOURCE_COL]]
    missing_cols = [c for c in reference_cols if c not in combined_df.columns]

    if missing_cols:
        raise ValueError("Reference columns missing after combine: {missing_cols}".format(missing_cols=missing_cols))

    if cfg.get("reference.authoritative_schema_for_reference", True):
        if extra_cols:
            logger.warning(
                "Dropping non-authoritative columns from reference (still kept in Bronze raws): %s",
                extra_cols
            )

        combined_df = combined_df.select(reference_cols + [SOURCE_COL])

    return combined_df


def extract_reference_values(
        cfg: Config,
        combined_df: pl.DataFrame
) -> dict[str, list[str]]:
    """
    Extract distinct reference values per validated column based on authoritative month.
    :param cfg: Config object to extract relevant variables
    :param combined_df: Polars dataframe containing combined CSVs with only columns by the determined authoritative
    schema.
    :return:
    """
    reference_month = cfg.require("reference.authoritative_dataset_month")
    month_col = cfg.require("scope.data_month_col")

    reference_rows_df = combined_df.filter(
        pl.col(month_col) == reference_month
    )
    reference_values_dict = {
        col: sorted(reference_rows_df[col].drop_nulls().unique().to_list())
        for col in cfg.require("validation.categorical_columns")
    }

    return reference_values_dict


def write_combined_df(
        cfg: Config,
        combined_df: pl.DataFrame,
) -> Path:
    """
    Write the combined dataset into a single parquet file.
    :param cfg: Config object to extract relevant variables
    :param combined_df: Polars dataframe containing combined CSVs with only columns by the determined authoritative
    schema.
    :return: Output path of written combined dataframe into parquet file.
    """
    output_dir = cfg.path("silver_combined", create=True)
    combined_filename = cfg.get("silver_combined_filename", "resale_combined")
    output_path = output_dir / "{filename}.parquet".format(filename=combined_filename)

    # Write combined polars dataframe into parquet with determined path above
    combined_df.write_parquet(output_path, compression="zstd")
    logger.info("Wrote %s (%d rows)", output_path, combined_df.height)

    return output_path


def summarize_ingestion_sizes(
        cfg: Config,
        frames: dict[str, pl.DataFrame],
        combined_df: pl.DataFrame,
        parquet_path: Path | None = None
) -> pl.DataFrame:
    """
    Calculate size summary per-file and combined to inform Spark vs Polars decisions.

    :param cfg: Config object to extract relevant variables
    :param frames: dict[str, pl.DataFrame] containing [filename, CSVs read into dataframe]
    :param combined_df: Polars dataframe containing combined CSVs with only columns by the determined authoritative
    schema.
    :param parquet_path:
    :return: Logs a summary of raw file sizes and row count whilst returning the combined dataframe.
    """
    raw_dir = cfg.path("bronze_raw")
    month_col = cfg.require("scope.data_month_col")
    start_month_year = cfg.require("scope.start_year_month")
    end_month_year = cfg.require("scope.end_year_month")

    rows = []
    for name, df in frames.items():
        in_scope_frame_height = df.filter(
            pl.col(month_col).is_between(pl.lit(start_month_year), pl.lit(end_month_year))
        ).height
        rows.append({
            "dataset": name,
            "rows": df.height,
            "cols": df.width - 1,
            "csv_size_mb": round((raw_dir/name).stat().st_size / 1e6, 2),
            "in_scope_rows": in_scope_frame_height
        })

    in_scope_total_height = combined_df.filter(
        pl.col(month_col).is_between(pl.lit(start_month_year), pl.lit(end_month_year))
    ).height

    rows.append({
        "dataset": "COMBINED (total)",
        "rows": combined_df.height,
        "cols": combined_df.width - 1,
        "csv_size_mb": round(sum(r["csv_size_mb"] for r in rows), 2),
        "in_scope_rows": in_scope_total_height
    })

    summary = pl.DataFrame(rows)

    parquet_message = (
        f"; parquet_size: {parquet_path.stat().st_size / 1e6:.1f} MB"
        if parquet_path
        else ""
    )

    logger.info(
        "Combined in-memory size: %.1f MB%s",
        combined_df.estimated_size("mb"),
        parquet_message
    )

    return summary


def run_combine(
        cfg: Config | None = None,
        write: bool = True
):
    """
    Read, combine and (based on whether the option is configured) write the combined dataset.

    :param cfg: Config object to extract relevant variables
    :param write: Boolean to determine whether to write the combined dataset or not. (Default: True)
    :return:
    """
    cfg = cfg or load_config()
    frames = read_raw_files(raw_path=cfg.path("bronze_raw"))
    combined_df = combine_frames(
        cfg=cfg,
        frames=frames,
    )
    if write:
        write_combined_df(
            cfg=cfg,
            combined_df=combined_df
        )
    return combined_df, frames


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    cfg = load_config()
    combined_df, frames = run_combine(
        cfg=cfg,
        write=False
    )
    print(extract_reference_values(cfg=cfg, combined_df=combined_df))

    print(summarize_ingestion_sizes(cfg=cfg, frames=frames, combined_df=combined_df))
