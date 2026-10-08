"""
Silver layer, validate categorical columns using the authoritative month/dataset. Additionally, use general data-quality
rules to help validate the data.
The two rule sets will be run seperately and then brought together.

General Data Quality rules:
    - MISSING_REQUIRED: a required column data is null or blank
    - DUPLICATE: row is a repeat of an earlier row (all columns except source_fiLe), first occurrence is kept
    - INVALID_NUMERIC: column supposed to be numeric but has data that isn't
Categorical Rules related to the authoritative dataset:
    - INVALID_{COLUMN}: value not in the authoritative month's set of values (nulls would also fail)

"""
from __future__ import annotations

import json
import logging
from pathlib import Path

import polars as pl

from src.config import Config, load_config

logger = logging.getLogger(__name__)


# -----------------------------------------
# Helper Functions
# -----------------------------------------

def _normalise(
        cfg: Config,
        expr: pl.Expr,
) -> pl.Expr:
    """
    Apply the configured matching normalization. To be used only for comparison.
    :param cfg: Config object to extract relevant variables
    :param expr:
    :return: polars expression depending on given polars expression and set configurations
    """
    # Based on config - if whitespace is to be stripped, it is normalised as so
    if cfg.get("validation.strip_whitespace", True):
        expr = expr.str.strip_chars()

    # Based on config - if case-sensitive validation is set to False it will normalize to uppercase
    if not cfg.get("validation.case_sensitive", True):
        expr = expr.str.to_uppercase()

    return expr


def _split_df_based_on_reject_rules(
        cfg: Config,
        df: pl.DataFrame,
        rules: dict[str, pl.Expr]
) -> tuple[pl.Dataframe, pl.Dataframe]:
    """

    :param cfg: Config object to extract relevant variables
    :param df:
    :param rules:
    :return:
    """
    reject_reason_col = cfg.require("validation.reject_reason_col")

    flagged_df = df.with_columns(
        pl.concat_str(
            [pl.when(fail_expression.fill_null(True)).then(pl.lit(fail_reason))
             for fail_reason, fail_expression in rules.items()],
            separator=";",
            ignore_nulls=True,
        ).alias(reject_reason_col)
    )

    # Since in flagged_df we used ignore_nulls = True, no reject reason => ""
    is_invalid_expression = pl.col(reject_reason_col) != ""
    valid_df = flagged_df.filter(
        ~is_invalid_expression).drop(reject_reason_col)
    quarantined_df = flagged_df.filter(
        is_invalid_expression
    )

    assert valid_df.height + quarantined_df.height == df.height,  "Row count drift"
    logger.info("General Data Quality rules validation: %d passed, %d quarantined",
                valid_df.height,
                quarantined_df.height
                )

    return valid_df, quarantined_df


# -----------------------------------------
# Categorical Authoritative Set Validation
# -----------------------------------------


def build_categorical_authoritative_check_rules(
        cfg: Config,
        authoritative_value_sets: dict[str, list[str]]
) -> dict[str, pl.Expr]:
    """
    Check INVALID_{COLUMN} rule where a value is not in the authoritative set.
    :param cfg: Config object to extract relevant variables
    :param authoritative_value_sets: dict[str, list[str]] of unique values per categorical column from authoritative
    dataset
    :return: dict[str, list[str]] of columns which had invalid data when compared to the authoritative_value_sets and
    the entries involved.
    """
    fail_expressions: dict[str, pl.Expr] = {}

    for col, allowed_values in authoritative_value_sets.items():
        # Since nulls don't exist in the authoritative set, they also fail the validation
        valid_data = _normalise(cfg=cfg, expr=pl.col(col)).is_in(allowed_values).fill_null(False)
        fail_expressions["INVALID_{}".format(col.upper())] = ~valid_data

    return fail_expressions


def build_authoritative_dataset_value_set(
        cfg: Config,
        combined_df: pl.DataFrame
) -> dict[str, list[str]]:
    """
    Filter combined df to reference month/dataset and extract list of unique values per categorical column.
    :param cfg: Config object to extract relevant variables
    :param combined_df: Polars dataframe containing combined CSVs with only columns by the determined authoritative
    schema.
    :return: dict[str, list[str]] of unique values per categorical column from authoritative dataset
    """
    reference_month = cfg.require("reference.authoritative_dataset_month")
    month_col = cfg.require("scope.data_month_col")

    reference_rows = combined_df.filter(
        pl.col(month_col) == pl.lit(reference_month)
    )

    if reference_rows.is_empty():
        raise ValueError("No rows found for reference month {ref_month}".format(ref_month=reference_month))

    value_sets: dict[str, list[str]] = {}
    for col in cfg.require("validation.categorical_columns"):
        values = reference_rows.select(_normalise(cfg=cfg, expr=pl.col(col))).to_series()
        value_sets[col] = sorted(values.drop_nulls().unique().to_list())
        logger.info("Reference %s: %d distinct values", col, len(value_sets[col]))

    return value_sets


def validate_categorical_columns(
        cfg: Config,
        combined_df: pl.DataFrame,
        authoritative_value_sets: dict[str, list[str]]
) -> tuple[pl.Dataframe, pl.Dataframe]:
    """
    Use validation rules to validate categorical columns and then split [df] into (valid, quarantined) polars
    dataframes. Quarantined frame gains "reject_reason" column.
    :param cfg: Config object to extract relevant variables
    :param combined_df: Polars dataframe containing combined CSVs with only columns by the determined authoritative
    schema.
    :param authoritative_value_sets: dict[str, list[str]] of unique values per categorical column from authoritative
    dataset
    :return:
    """
    valid_df, quarantined_df = _split_df_based_on_reject_rules(
        cfg=cfg, df=combined_df, rules=build_categorical_authoritative_check_rules(
            cfg=cfg,
            authoritative_value_sets=authoritative_value_sets)
    )

    logger.info("Validation: %d valid, %d quarantined", valid_df.height, quarantined_df.height)

    return valid_df, quarantined_df


# -----------------------------------------
# General Data Quality Rules
# -----------------------------------------


def is_blank(col: str) -> pl.Expr:
    """
    Generate polars expression depending on given column to check for blanks/nulls.
    :param col: column name being checked for blanks/nulls
    :return: polars Expressions to determine if a column is blank or null
    """
    return pl.col(col).is_null() | (pl.col(col).str.strip_chars() == "")


def build_data_quality_rules(
        cfg: Config,
        df: pl.DataFrame
) -> dict[str, pl.Expr]:
    """
    Build the general data quality rules {reject_reason: failing_condition} dict with the configured settings
    in config.validation.
    :param cfg: Config object to extract relevant variables
    :param df: polars Dataframe to be checked against data quality rules
    :return: dict[str, pl.Expr] with dict[reject_reason, failing_condition polars expression].
    """
    required_columns_available = [c for c in cfg.require("validation.required_columns") if c in df.columns]
    reject_reason_col = cfg.require("validation.reject_reason_col")

    numeric_checks_resale_price_min = cfg.require("validation.numeric_checks_resale_price_min")
    numeric_checks_floor_area_sqm_min = cfg.require("validation.numeric_checks_floor_area_sqm_min")

    data_lease_commence_date_col = cfg.require("scope.data_lease_commence_date_col")
    data_resale_price_col = cfg.require("scope.data_resale_price_col")
    data_floor_area_sqm_col = cfg.require("scope.data_floor_area_sqm_col")
    data_month_col = cfg.require("scope.data_month_col")
    scope_start_month_year = cfg.require("scope.start_year_month")
    scope_end_month_year = cfg.require("scope.end_year_month")

    resale_price_col_data = pl.col(data_resale_price_col).cast(pl.Float64, strict=False)
    floor_area_col_data = pl.col(data_floor_area_sqm_col).cast(pl.Float64, strict=False)
    lease_year_col_data = pl.col(data_lease_commence_date_col).cast(pl.Int32, strict=False)
    sale_year_month_col_data = pl.col(data_month_col).str.slice(0, 4).cast(pl.Int32, strict=False)

    # Check which entries have null or blank data
    missing_required_data = pl.any_horizontal([is_blank(c) for c in required_columns_available])

    # Check for out of scope data (Configurable in config.yaml)
    out_of_scope_data = ~pl.col(data_month_col).is_between(
        pl.lit(scope_start_month_year),
        pl.lit(scope_end_month_year)
    )

    # Check for faulty numerical data
    invalid_numerical_data = (
        resale_price_col_data.is_null() | (resale_price_col_data <= numeric_checks_resale_price_min)
        | floor_area_col_data.is_null() | (floor_area_col_data <= numeric_checks_floor_area_sqm_min)
        | lease_year_col_data.is_null() | (lease_year_col_data > sale_year_month_col_data)
    )

    # Determine cols for duplicate check
    dup_cols = [c for c in df.columns if c != cfg.require("scope.source_filename_col")]

    # Create window function - sort influenced by whether to keep "first" or "last"
    duplicate_keep = cfg.get("validation.duplicate_keep", True)
    position_data = pl.int_range(pl.len()).over(dup_cols)

    if duplicate_keep == "last":
        position_data = pl.int_range(pl.len()).over(dup_cols)

    is_duplicate_data = position_data > 0

    data_quality_rules = {
        "MISSING_REQUIRED": missing_required_data.fill_null(True),
        "INVALID_NUMERIC": invalid_numerical_data.fill_null(True),
        "DUPLICATE": is_duplicate_data,
        "OUT_OF_SCOPE_DATE": out_of_scope_data,
    }

    return data_quality_rules


def apply_data_quality_rules(
        cfg: Config,
        combined_df: pl.DataFrame,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """
    Apply the rules calculated in build_data_quality_rules() to the data and split into valid_data and
    quarantined_data.
    :param cfg: Config object to extract relevant variables
    :param combined_df: Polars dataframe containing combined CSVs with only columns by the determined authoritative
    schema.
    :return: tuple[Dataframe, Dataframe] with tuple[valid, quarantined] data.
    """
    valid_df, quarantined_df = _split_df_based_on_reject_rules(cfg=cfg, df=combined_df, rules=build_data_quality_rules(
        cfg=cfg, df=combined_df
    ))

    logger.info("General Data Quality rules: %d valid, %d quarantined", valid_df.height,
                quarantined_df.height)

    return valid_df, quarantined_df


# -----------------------------------------
# Run and combine validation rule sets
# -----------------------------------------

def run_validation(
        cfg: Config,
        combined_df: pl.DataFrame,
        authoritative_value_sets: dict[str, list[str]] | None = None,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """
    Run categorical and general data quality rules together.
    :param cfg: Config object to extract relevant variables
    :param combined_df: Polars dataframe containing combined CSVs with only columns by the determined authoritative
    schema.
    :param authoritative_value_sets: dict[str, list[str]] of unique values per categorical column from authoritative
    dataset
    :return: tuple[Dataframe, Dataframe] with tuple[valid, quarantined] data from both sets of validation rules.
    """
    if authoritative_value_sets is None:
        authoritative_value_sets = build_authoritative_dataset_value_set(
            cfg=cfg,
            combined_df=combined_df,
        )

    combined_quality_rules = {
        **build_data_quality_rules(cfg=cfg, df=combined_df),
        **build_categorical_authoritative_check_rules(cfg=cfg, authoritative_value_sets=authoritative_value_sets)
    }

    valid_df, quarantined_df = _split_df_based_on_reject_rules(
        cfg=cfg,
        df=combined_df,
        rules=combined_quality_rules,
    )
    assert valid_df.height + quarantined_df.height == combined_df.height, "Row drift has occurred, check validation.py"
    logger.info("Validation: %d valid, %d quarantined", valid_df.height, quarantined_df.height)
    return valid_df, quarantined_df

# -----------------------------------------
# Data Validation Reports
# -----------------------------------------

def quarantine_reason_counts(
        cfg: Config,
        quarantined_df: pl.DataFrame,
) -> pl.DataFrame:
    """
    Rows per individual reject reason (a row with two applicable reasons counts once for each).
    :param cfg: Config object to extract relevant variables
    :param quarantined_df: polars DataFrame of rows that were quarantined
    :return: dataframe with [reject_reason_name, row_count]
    """
    reject_reason_col = cfg.require("validation.reject_reason_col")

    if quarantined_df.is_empty():
        # row_count unsigned since it is empty
        return pl.DataFrame(schema={"reject_reason_name": pl.String, "row_count": pl.UInt16})

    # Transform quarantined_df into intended reason count df
    quarantined_df = (quarantined_df.select(
        pl.col(reject_reason_col).str.split(";").alias("reject_reason_name"),)
        .explode("reject_reason_name")
        .group_by("reject_reason_name").len()
        .rename({"len": "row_count"}).sort("row_count", descending=True)
                      )

    return quarantined_df


def summarize_rejections(
        cfg: Config,
        quarantined_df: pl.Dataframe,
        authoritative_value_sets: dict[str, list[str]]
) -> pl.Dataframe:
    """
    Collate which values caused rejections on a per-column basis for quarantine details report.
    :param cfg: Config object to extract relevant variables
    :param quarantined_df: polars DataFrame of rows that were quarantined
    :param authoritative_value_sets: dict[str, list[str]] of unique values per categorical column from authoritative
    dataset
    :return: dataframe with columns [column, value, rows]
    """
    rows = []
    reject_reason_col = cfg.require("validation.reject_reason_col")

    for col, allowed in authoritative_value_sets.items():
        reason = "INVALID_{col_name}".format(col_name=col.upper())
        failed_df = quarantined_df.filter(
            pl.col(reject_reason_col).str.contains(reason)
        )

        if failed_df.is_empty():
            continue

        # Group failed rows dataframe and collate to get count per column and value
        counts = (
            failed_df.group_by(col).len().sort("len", descending=True)
            .rename({col: "value", "len": "row_count"})
            .with_columns(
                pl.lit(col).alias("column")
            )
            .select("column", "value", "row_count")
        )
        rows.append(counts)

    if not rows:
        return pl.Dataframe(schema={"column": pl.String, "value": pl.String, "row_count": pl.String})

    return pl.concat(rows)

# -----------------------------------------
# Data Validation Write
# -----------------------------------------

def write_validation_outputs(
        valid_df: pl.DataFrame,
        quarantined_df: pl.DataFrame,
        authoritative_value_sets: dict[str, list[str]],
        cfg: Config | None = None
) -> dict[str, Path]:
    """
    Write valid_df and quarantined_df into parquet files and the determined authoritative reference values as JSON.
    :param valid_df: polars DataFrame of rows that were determined to be valid
    :param quarantined_df: polars DataFrame of rows that were determined to be quarantined
    :param authoritative_value_sets: dict[str, list[str]] of unique values per categorical column from authoritative dataset
    :param cfg: Config object to extract relevant variables
    :return: dict[str, Path] with string either valid, quarantined or reference determining which object and Path is
    the path it was written to.
    """
    cfg = cfg or load_config()
    paths = {
        "valid": cfg.path("silver_cleaned", create=True) / "{filename}.parquet".format(
            filename=cfg.require("outputs.silver_cleaned_filename")),
        "quarantined": cfg.path("silver_quarantined", create=True) / "{filename}.parquet".format(
            filename=cfg.require("outputs.silver_quarantined_filename")),
        "reference": cfg.path("profiling", create=True) / "authoritative_reference_values.json"
    }

    # Write valid rows to "valid" path above
    valid_df.write_parquet(paths["valid"], compression="zstd")

    # Write quarantined rows to "quarantined" path above
    quarantined_df.write_parquet(paths["quarantined"], compression="zstd")

    # Write reference values from authoritative set to json with easier readability indent = 2 and utf-8 encoding
    paths["reference"].write_text(json.dumps(authoritative_value_sets, indent=2), encoding="utf-8")

    for name, p in paths.items():
        logger.info("Wrote %s -> %s", name, p)

    return paths




