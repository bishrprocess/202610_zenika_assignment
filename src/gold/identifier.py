"""
Gold Layer: Create resale identifier based on requirements.
- Identifier must be built from VALIDATED DATA
- Structure:
    1) Constant prefix - config.identifier.prefix
    2) first 3 digits of block, non-digits removed and left-padded with zeroes when not enough digits
    3) first 3 digits of the avg resale price of the data group configured (default: year-month, town, flat_type)
    4) year-month of the row, YYMM (2012-01 -> 1201)
    5) first character of the town
- Uniqueness:
    Determined identifier could match across rows which is a problem. To solve this, rows are NOT DROPPED,
    but instead get a sequence-suffix (-001, -002...)
    - rows are ranked inside each colliding example and then by every remaining columns so that it is a total
    order.
        - The result DOES NOT DEPEND on the input row order so that a re-run would yield the same results.
    - Fully unique identifiers end up with no such suffix.
    - Suffix-added identifiers can never be equal to plain ones due to having a '-'
- Possible edge cases and assumptions:
    - block with no digits becomes 000
    - block with more than 3 digits only keeps first 3
    - group resale price average includes anomaly prices by default as they are not removed
    - group resale price average is truncated, not rounded.
"""
from __future__ import annotations

import logging
from pathlib import Path

import polars as pl

from src.config import Config, load_config

logger = logging.getLogger(__name__)

# Data month pattern regex
MONTH_PATTERN = r"^\d{4}-\d{2}$"

def check_valid_input(
        cfg: Config,
        df: pl.DataFrame,
        avg_resale_price_group_cols: list[str],
) -> None:
    """
    Throw error early if the data cannot produce a valid identifier. If this happens, data quality needs to be
    checked again/whether it was run correctly.
    :param cfg: Config object to extract relevant variables.
    :param df: Validated data frame that has passed the checks in validation.py.
    :param avg_resale_price_group_cols: list of cols with which to group average resale price.
    :return: None, will throw a ValueError in case of data issues.
    """
    data_month_col = cfg.require("scope.data_month_col")
    data_block_col = cfg.require("scope.data_block_col")
    data_town_col = cfg.require("scope.data_town_col")
    data_resale_price_col = cfg.require("scope.data_resale_price_col")

    id_col = cfg.require("identifier.resale_id_col")

    # Necessary cols as determined by requirements and reflect in config
    necessary_cols = {
        data_month_col,
        data_block_col,
        data_town_col,
        data_resale_price_col,
        *avg_resale_price_group_cols,

    }

    absent_necessary_cols = sorted(necessary_cols - set(df.columns))
    if absent_necessary_cols:
        raise ValueError("Columns required for the identifier are missing: {}".format(absent_necessary_cols))

    # Check for id col in given df in case output is re-run accidentally
    if id_col in df.columns:
        raise ValueError("Identifier column {} already exists in given df. Cannot overwrite.".format(id_col))

    # Check whether month col values are in the intended format
    incorrect_format_month = df.filter(
        ~pl.col(data_month_col).str.contains(MONTH_PATTERN).fill_null(False)
    ).height
    if incorrect_format_month:
        raise ValueError("{row_count} row(s) have values that aren't in the correct format {col_name}".format(
            row_count=incorrect_format_month,
            col_name=data_month_col
        ))

    # Checks already handled in validation.py but kept for robustness
    faulty_resale_price = df.filter(
        pl.col(data_resale_price_col).cast(pl.Float64, strict=False).is_null()
    ).height
    if faulty_resale_price:
        raise ValueError("{row_count} row(s) have a non-numeric value in {col_name}".format(
            row_count=faulty_resale_price,
            col_name=data_resale_price_col)
        )

    blank_town = df.filter(
        pl.col(data_town_col).is_null() | (pl.col(data_town_col).str.strip_chars() == "")
    ).height
    if blank_town:
        raise ValueError("{row_count} row(s) have a blank/null value in {col_name}".format(
            row_count=blank_town,
            col_name=data_town_col
        ))


def base_identifier_expression(
        cfg: Config,
) -> pl.Expr:
    """
    Create expression for the base identifier before uniqueness operations are enforced.
    :param cfg: Config object to extract relevant variables.
    :return: polars expression
    """
    data_month_col = cfg.require("scope.data_month_col")
    data_block_col = cfg.require("scope.data_block_col")
    data_town_col = cfg.require("scope.data_town_col")
    prefix = cfg.require("identifier.prefix")
    block_digits = cfg.require("identifier.block_digits")
    resale_price_digits = cfg.require("identifier.price_digits")
    avg_resale_price_col = cfg.require("identifier.avg_resale_price_col")

    block_segment = (
        pl.col(data_block_col).str.replace_all(r"[^0-9]", "")
        .str.slice(0, block_digits)
        .str.zfill(block_digits)
    )

    resale_price_segment = (
        pl.col(avg_resale_price_col).floor().cast(pl.Int64).cast(pl.String)
        .str.slice(0, resale_price_digits)
        .str.zfill(resale_price_digits)
    )

    period_segment = (pl.col(data_month_col).str.slice(2, 2)
                      + pl.col(data_month_col).str.slice(5, 2))

    town_segment = pl.col(data_town_col).str.strip_chars().str.slice(0,1).str.to_uppercase()

    return pl.concat_str(
        [pl.lit(prefix), block_segment, resale_price_segment, period_segment, town_segment]
    )


def add_resale_identifier(
        cfg: Config,
        validated_df: pl.DataFrame
) -> pl.DataFrame:
    """
    Add unique resale identifier column to the validated dataframe. No rows are dropped or re-ordered whilst doing so.
    :param cfg: Config object to extract relevant variables.
    :param validated_df: dataframe of validated rows after validation.py is run.
    :return: dataframe with the identifier column appended.
    """
    id_col = cfg.require("identifier.resale_id_col")
    base_id_col = cfg.require("identifier.resale_base_id_col")
    avg_resale_price_group_cols = list(cfg.require("identifier.avg_group_cols"))
    tie_breaker_cols = list(cfg.get("identifier.uniqueness.tie_breaker_cols"))
    group_size_col = cfg.require("identifier.group_size_col")

    suffix_separator = cfg.require("identifier.uniqueness.suffix_separator")
    row_num_col = cfg.require("identifier.row_num_col")
    data_resale_price_col = cfg.require("scope.data_resale_price_col")
    avg_resale_price_col = cfg.require("identifier.avg_resale_price_col")
    sequence_col = cfg.require("identifier.sequence_col")

    if cfg.require("identifier.uniqueness.strategy") != "sequence_suffix":
        raise ValueError("Strategy must be set to 'sequence_suffix' for uniqueness identifier.")

    check_valid_input(
        cfg=cfg,
        df=validated_df,
        avg_resale_price_group_cols=avg_resale_price_group_cols,
    )

    # Calculate base identifier necessary cols - avg resale price per group and then use to generate base identifier
    base_identifier_df = (
        validated_df.with_row_index(row_num_col)
        .with_columns(
            pl.col(data_resale_price_col)
            .cast(pl.Float64)
            .mean().over(avg_resale_price_group_cols)
            .alias(avg_resale_price_col)
        ).with_columns(
            base_identifier_expression(
                cfg=cfg
            ).alias(base_id_col)
        )
    )

    # Ranks rows inside each identified group - use tiebreaker columns when necessary
    cols_not_in_tie_breaker = [c for c in validated_df.columns if c not in tie_breaker_cols]
    ranked_df = (
        base_identifier_df.sort([
            base_id_col, *tie_breaker_cols, *cols_not_in_tie_breaker
        ], nulls_last=True)
    ).with_columns(
        (pl.int_range(pl.len()).over(base_id_col) + 1).alias(sequence_col),
        pl.len().over(base_id_col).alias(group_size_col),
    )

    # Suffix only rows that share an identifier
    largest_group = int(ranked_df[group_size_col].max() or 1)
    suffix_width = max(int(cfg.require("identifier.uniqueness.suffix_width")), len(str(largest_group)))
    unique_identifier_expression = (
        pl.when(pl.col(group_size_col) == 1)
        .then(pl.col(base_id_col))
        .otherwise(pl.col(base_id_col) + pl.lit(suffix_separator) + pl.col(sequence_col).cast(pl.String)
                   .str.zfill(suffix_width))
    )

    # add identifier to id_col and remove helper columns
    output_df = (
        ranked_df.with_columns(
            unique_identifier_expression.alias(id_col))
        .sort(row_num_col)
        .drop(row_num_col, base_id_col, sequence_col, group_size_col, avg_resale_price_col
        )
    )

    # Check that row count is still the same after
    assert output_df.height == validated_df.height, "Row count drift occurred, check operations."
    assert output_df[id_col].n_unique() == output_df.height, "Resale identifiers are not unique"
    logger.info("Added %s to %d rows", id_col, output_df.height)
    return output_df


def summarize_identifier_collisions(
        cfg: Config,
        df: pl.DataFrame
) -> dict[str, int]:
    """
    Describe how many rows needed a uniqueness suffix and othe related statistics.
    :param cfg: Config object to extract relevant variables.
    :param df: dataframe returned by add_resale_identifier()
    :return: dict ith total rows, suffixed rows, colliding identifiers and the largest group size.
    """
    id_col = cfg.require("identifier.resale_id_col")
    suffix_separator = cfg.require("identifier.uniqueness.suffix_separator")

    ids_data = df[id_col]
    suffixed_ids_data = ids_data.filter(
        ids_data.str.contains(suffix_separator, literal=True)
    )

    if suffixed_ids_data.is_empty():
        return {
            "rows": df.height, "suffixed_rows": 0, "colliding_identifiers": 0, "largest_group_size": 1
        }

    identifier_group_sizes = suffixed_ids_data.str.split(suffix_separator).list.first().value_counts()["count"]
    return {
        "rows": df.height,
        "suffixed_identifier_count": suffixed_ids_data.len(),
        "colliding_identifiers_count": identifier_group_sizes.len(),
        "largest_group_size": int(identifier_group_sizes.max()),
    }


def write_transformed_identifier_df(
        cfg: Config,
        transformed_df: pl.DataFrame,
) -> Path:
    """
    Write the transformed output dataframe as parquet file.
    :param cfg: Config object to extract relevant variables.
    :param transformed_df: dataframe containing the identifier column with uniqueness.
    :return: path which parquet was written to.
    """
    identifier_df_path = cfg.path("gold_transformed", create=True) / "{filename}.parquet".format(
        filename=cfg.require("outputs.gold_transformed_filename")
    )
    transformed_df.write_parquet(identifier_df_path, compression="zstd")
    logger.info("Wrote transformed output -> %s", identifier_df_path)
    return identifier_df_path

