"""
Silver layer: calculate remaining lease as of fixed date in Config

Assumptions (To be moved or mentioned in output documentation)
- "lease_commence_date" is a year so the default beginning would have to be the first of said year. (default January)
- lease starts with 99 Years according to assignment
- leases that have already ended, if any, would be given a 0 (not a -ve value)
- option to pick commence month of year?
- should calculate
    - [For easy future aggregations] remaining_years (int), remaining_months (int)
    - [For easy readability] remaining lease text (Format from newer datasets showed '## years ## months'

"""
from __future__ import annotations

import datetime as dt
import polars as pl

from src.config import Config, load_config


def calculate_remaining_lease(
        df: pl.DataFrame,
        cfg: Config | None = None,
) -> pl.DataFrame:
    """
    Calculate remaining lease according to assignment and assumptions. Remaining lease will be calculated in years and
    months then in the text form required for easy readability and interpretation.
    :param cfg: Config object to extract relevant variables
    :param df: polars Dataframe of combined data, preferably post validation.
    :return: polars Dataframe with remaining lease calculated as determined by assumptions.
    """
    cfg = cfg or load_config()

    remaining_years_col = cfg.require("calc_remaining_lease.remaining_lease_years_col")
    remaining_months_col = cfg.require("calc_remaining_lease.remaining_lease_months_col")
    remaining_text_col = cfg.require("calc_remaining_lease.remaining_lease_text_col")

    columns_already_in_dataframe = [c for c in (remaining_years_col, remaining_months_col, remaining_text_col)
                                    if c in df.columns]
    if columns_already_in_dataframe:
        raise ValueError("Output column(s) already exist in the given data: {col_alr_in_df}".format(
            col_alr_in_df=columns_already_in_dataframe)
        )

    lease_full_term_years = int(cfg.require("calc_remaining_lease.lease_full_term_years"))
    lease_calc_period_end_date = dt.date.fromisoformat(cfg.require("calc_remaining_lease.lease_calc_period_end_date"))
    year_start_month = int(cfg.require("calc_remaining_lease.lease_year_start_month"))

    if year_start_month > 12:
        raise ValueError("Lease commence date starting month cannot be greater than 12")

    # Calculate month index:  year * 12 + (month - 1)
    lease_calc_period_end_date_index = lease_calc_period_end_date.year * 12 + (lease_calc_period_end_date.month - 1)
    lease_commencement = pl.col("lease_commence_date").cast(pl.Int32, strict=False)
    lease_end_index = (lease_commencement + lease_full_term_years) * 12 + (year_start_month - 1)

    # Calculate remaining lease - pl.lit(0) for when the calculation is negative (i.e. lease is expired)
    remaining_lease = pl.max_horizontal(lease_end_index - lease_calc_period_end_date_index, pl.lit(0))

    df = df.with_columns(
        remaining_lease.alias("_remaining_months")
    ).with_columns(
        (pl.col("_remaining_months") // 12).alias(remaining_years_col),
        (pl.col("_remaining_months") % 12).alias(remaining_months_col),
    ).with_columns(
        pl.format(
            "{} years {} months",
            pl.col(remaining_years_col),
            pl.col(remaining_months_col),
        ).alias(remaining_text_col)
    ).drop(
        "_remaining_months"
    )

    return df
