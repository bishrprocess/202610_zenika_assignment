"""
Silver layer, data profiling.

"""
from __future__ import annotations

import logging
from pathlib import Path

import polars as pl

from src.config import Config, load_config

logger = logging.getLogger(__name__)

# Columns determined from authoritative set
DEFAULT_NUMERICS_COLS = ["floor_area_sqm", "lease_commence_date", "resale_price"]


def _typed_for_profiling(
        df: pl.DataFrame,
        numeric_cols: list[str]
):
    """
    Returns a pandas copy of supplied polars dataframe with numeric columns case.
    To be used in profiling only.
    :param df:
    :param numeric_cols:
    :return: Pandas Dataframe copy of df with numeric columns cast to Float64
    """
    numerical_present_cols = [c for c in numeric_cols if c not in df.columns]
    type_cast_df = df.with_columns(
        [pl.col(c).cast(pl.Float64, strict=False) for c in numerical_present_cols]
    )

    return type_cast_df.to_pandas()


def generate_ydata_profile_reference_dataset(
        cfg: Config,
        combined_df: pl.DataFrame,
        minimal: bool = False,
) -> dict[str, Path]:
    """
    Profile the authoritative dataset (month) and write the outputted reports.
    :param cfg: Config object to extract relevant variables
    :param combined_df: Polars dataframe containing combined CSVs with only columns by the determined authoritative
    :param minimal: boolean, depending on hardware and time, choose between more extensive report or minimal report
    :return: dict containing profile file type as key and path to profile file as value
    """
    # Import in function as this will not be frequuently re-used and only necessary here - heavy dependency
    from ydata_profiling import ProfileReport

    cfg = cfg or load_config()
    reference_month = cfg.require("reference.authoritative_dataset_month")

    month_col = cfg.require("scope.data_month_col")
    source_filename_col = cfg.require("scope.source_filename_col")

    # Filter combined data to reference timeframe and drop source filename column - added by us
    reference_filtered_df = combined_df.filter(
        pl.col(month_col) == pl.lit(reference_month)).drop(source_filename_col)


    if reference_filtered_df.is_empty():
        raise ValueError("No rows found for reference month {ref_month}".format(ref_month=reference_month))
    logger.info("Profiling %d rows for %s", reference_filtered_df.height, reference_month)

    numeric_cols = cfg.get("profiling.authoritative_dataset_numeric_columns", DEFAULT_NUMERICS_COLS)

    # Convert reference df from polars dataframe to pandas for ydata profiling and cast numerical cols
    pandas_reference_df = _typed_for_profiling(
        df=reference_filtered_df,
        numeric_cols=numeric_cols,
    )

    # Generate data profile with ydata ProfileReport
    data_profile_report = ProfileReport(
        df=pandas_reference_df,
        title="HDB Resale - Authoritative Set ({ref_month})".format(ref_month=reference_month),
        minimal=minimal,
        progress_bar=False
    )

    # Save data profile results - html and json
    output_dir = cfg.path("profiling", create=True)
    html_path = output_dir / "profile_reference_{ref_month}.html".format(ref_month=reference_month)
    json_path = output_dir / "profile_reference_{ref_month}.json".format(ref_month=reference_month)
    data_profile_report.to_file(output_file=html_path)
    data_profile_report.to_file(output_file=json_path)
    logger.info("Wrote %s and %s", html_path.name, json_path.name)

    return {"html": html_path, "json": json_path}


def generate_ydata_profile(
        df: pl.DataFrame,
        profile_name: str,
        minimal: bool = False,
        cfg: Config | None = None,
) -> dict[str, Path]:
    """
    Profile given dataframe and write the outputted reports.
    :param cfg: Config object to extract relevant variables
    :param df: Polars dataframe to be profile by ydata
    :param profile_name: string name of profile that will be used for the file name
    :param minimal: boolean, depending on hardware and time, choose between more extensive report or minimal report
    :return: dict containing profile file type as key and path to profile file as value
    """
    # Import in function as this will not be frequently re-used and only necessary here - heavy dependency
    from ydata_profiling import ProfileReport

    cfg = cfg or load_config()

    if df.is_empty():
        raise ValueError("No rows found for in given dataframe")

    logger.info("Profiling %d rows for %s", df.height, profile_name)

    numeric_cols = cfg.get("profiling.authoritative_dataset_numeric_columns", DEFAULT_NUMERICS_COLS)

    # Convert reference df from polars dataframe to pandas for ydata profiling and cast numerical cols
    pandas_reference_df = _typed_for_profiling(
        df=df,
        numeric_cols=numeric_cols,
    )

    # Generate data profile with ydata ProfileReport
    data_profile_report = ProfileReport(
        df=pandas_reference_df,
        title="HDB Resale - Profile ({name})".format(name=profile_name),
        minimal=minimal,
        progress_bar=False
    )

    # Save data profile results - html and json
    output_dir = cfg.path("profiling", create=True)
    html_path = output_dir / "profile_reference_{name}.html".format(name=profile_name)
    json_path = output_dir / "profile_reference_{name}.json".format(name=profile_name)
    data_profile_report.to_file(output_file=html_path)
    data_profile_report.to_file(output_file=json_path)
    logger.info("Wrote %s and %s", html_path.name, json_path.name)

    return {"html": html_path, "json": json_path}


def profile_stage(
        df: pl.DataFrame,
        stage: str
) -> pl.DataFrame:
    """
    Polars-native per column metrics with stage to convey pipeline stage.
    :param df: Polars Dataframe
    :param stage: String, Stage in data pipeline
    :return: Polars Dataframe with per-column metrics of given dataframe.
    Metrics include:
        - null_count
        - null_percentage
        - row_count
        - distinct_count
    """
    rows = []
    for col in df.columns:
        data_column = df[col]
        rows.append({
            "stage": stage,
            "column": col,
            "row_count": df.height,
            "null_count": data_column.null_count(),
            "null_percentage": round(data_column.null_count() / df.height * 100, 3) if df.height else 0.0,
            "distinct_count": data_column.n_unique(),
        })

    return pl.DataFrame(rows)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    cfg = load_config()

    combined_data_dir = cfg.path("silver_combined")
