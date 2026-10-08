"""
Silver layer: determine and flag potentially anomalous resale prices.

Assumptions:
- assignment mentions identifying, not removing -> anomalies might still be legitimate
-

Heuristics:
1) Price per sqm outside configured bounds (min/max price per sqm)
    - the bounds could be calculated based on a mean over a group of columns but that would be quite similar to the
    heuristic in 2)
    - Therefore, giving the option to define the bounds in case specific value-based anomaly detection
    on price per sqm is needed.
2) Peer-group outlier test (Group on factors with possible impact on the price - town, flat_type, year (extracted from
month) ...)
    - determine a minimum group size and fall back group (check whether flat_type + year would work) to avoid smaller
    groups creating inappropriate thresholds
    - IQR is preferred for bigger peer groups, whereas modified zscore is better suited to smaller groups
        - IQR gives a binary answer whether a value falls outside the bounds whereas modified zscore gives a "distance"
        from the calculated MAD (Median Absolute Deviation)
        - allowing the choice between both heuristics depending on config makes sense as data needs might evolve or
        change
        - generally, if group sizes are 5 < x <= 12: modified zscore; 12 < x < 30 both; 30 < x IQR
            - somewhat of an oversimplification but should work for this use case

"""
from __future__ import annotations

import logging

import polars as pl

from src.config import Config, load_config

logger = logging.getLogger(__name__)

def peer_group_col_keys(
        cfg: Config,
) -> tuple[list[str], list[str]]:
    """
    Extract peer group columns from config and return tuple with general operation column list and fallback column list.
    :param cfg: Config object to extract relevant variables
    :return: tuple with general operation column list and fallback column list
    """
    peer_group_by_cols = list(cfg.require("anomaly.peer_group_by_cols"))
    peer_fallback_cols = list(cfg.require("anomaly.peer_fallback_cols"))
    return peer_group_by_cols, peer_fallback_cols


def flag_resale_price_anomalies(
        cfg: Config,
        df: pl.DataFrame
) -> pl.DataFrame:
    """
    Calculate resale price related anomaly columns.
    :param cfg: Config object to extract relevant variables
    :param df: polars Dataframe of combined data, preferably post validation.
    :return:
    """
    anomaly_flag_col = cfg.require("anomaly.flag_col")
    anomaly_reasons_col = cfg.require("anomaly.anomaly_reasons_col")
    anomaly_method = cfg.require("anomaly.anomaly_method")
    if anomaly_method not in ["iqr", "modified_zscore"]:
        raise ValueError("Configured anomaly method is not supported: anomaly_method {anomaly_method}".format(
            anomaly_method=anomaly_method
        ))

    min_group_size = int(cfg.require("anomaly.min_group_size"))

    group_by_cols, fallback_cols = peer_group_col_keys(cfg)

    # Calculate resale price per sqm - cast relevant cols and extract year from month
    calc_df = df.with_columns(
        pl.col("resale_price").cast(pl.Float64, strict=False).alias("resale_price_num"),
        pl.col("floor_area_sqm").cast(pl.Float64, strict=False).alias("_area_sqm"),
        pl.col("month").str.slice(0, 4).alias("year")
    ).with_columns(
        (pl.col("resale_price_num") / pl.col("_area_sqm")).alias("resale_price_per_sqm")
    )

    # Count entries per group
    price_col_data = pl.col("resale_price_num")
    entry_count_per_peer_group = price_col_data.count().over(group_by_cols)

    # Fall-back function for peer group anomaly
    def peer_group_stats(expr_func) -> pl.Expr:
        """
        Fall-back to broader group - fallback_cols - when too small compared to configuration.
        :param expr_func:
        :return: polars Expression
        """
        return pl.when(entry_count_per_peer_group >= min_group_size).then(expr_func().over(group_by_cols)
                                                 ).otherwise(expr_func().over(fallback_cols))

    if anomaly_method == "iqr":
        k = float(cfg.require("anomaly.iqr_multiplier"))

        # Calculate resale price quantiles and bounds
        q1 = peer_group_stats(lambda: price_col_data.quantile(0.25))
        q3 = peer_group_stats(lambda: price_col_data.quantile(0.75))
        iqr = q3 - q1
        lower_bound_data = price_col_data < q1 - k * iqr
        upper_bound_data = price_col_data > q3 + k * iqr
    elif anomaly_method == "modified_zscore":
        zscore_threshold = float(cfg.get("anomaly.modified_zscore_threshold", 3.5))
        calc_df = calc_df.with_columns(
            peer_group_stats(lambda: price_col_data.median()).alias("_median_price")
        )
        calc_df = calc_df.with_columns(
            peer_group_stats(lambda: (price_col_data - pl.col("_median_price"))).alias("_mad")
        )

        # MAD can be 0 (no spread in group) - no flag on zero denominator
        z = 0.6745 * (price_col_data - pl.col("_median_price")) / pl.col("_mad")
        lower_bound_data = (pl.col("_mad") > 0) & (z < -threshold)
        upper_bound_data = (pl.col("_mad") > 0) & (z > threshold)

    anomaly_reasons = [
        pl.when(lower_bound_data.fill_null(False)).then(pl.lit("RESALE_PRICE_LOW_FOR_PEERS")),
        pl.when(upper_bound_data.fill_null(False)).then(pl.lit("RESALE_PRICE_HIGH_FOR_PEERS")),
    ]

    # Check config whether price per sqm is used for anomaly determination - default False
    if cfg.get("anomaly.enable_resale_price_per_sqm", False):
        low_bound_resale_price_per_sqm = float(cfg.require("anomaly.low_bound_resale_price_per_sqm"))
        high_bound_resale_price_per_sqm = float(cfg.require("anomaly.high_bound_resale_price_per_sqm"))

        anomaly_reasons.append(
            pl.when(((pl.col("resale_price_per_sqm") < low_bound_resale_price_per_sqm) |
                    (pl.col("resale_price_per_sqm") > high_bound_resale_price_per_sqm)).fill_null(False)
                    ).then(pl.lit("PRICE_PER_SQM_OUT_OF_BOUNDS"))
        )

    calc_df = calc_df.with_columns(
        pl.concat_str(anomaly_reasons, separator=";", ignore_nulls=True).alias(anomaly_reasons_col)
    ).with_columns(
            (pl.col(anomaly_reasons_col) != "").alias(anomaly_flag_col)
    )


    # Drop helper cols that are no longer needed
    calc_df = calc_df.drop([c for c in ("_area_sqm", "_median_price", "_mad") if c in calc_df.columns])

    logger.info("Flagged %d of %d rows as resale price anomalies",
                calc_df.filter(pl.col(anomaly_flag_col)).height,
                calc_df.height
                )
    return calc_df


def summarize_anomalies_report(
        cfg: Config,
        df: pl.DataFrame
) -> pl.DataFrame:
    """
    Anomaly counts per reason determined.
    :param cfg: Config object to extract relevant variables
    :param df: polars Dataframe of data that has been run through the anomaly detection function.
    :return: polars Dataframe with row counts per anomaly reason detected for supplied dataframe.
    """
    anomaly_flag_col = cfg.require("anomaly.flag_col")
    anomaly_reasons_col = cfg.require("anomaly.anomaly_reasons_col")
    df = (df.filter(pl.col(anomaly_flag_col))
          .group_by(anomaly_reasons_col).len().sort("len", descending=True)
          .rename({"len": "row_count"})
    )
    return df


def split_anomalies(
        cfg: Config,
        df: pl.DataFrame
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """
    Apply anomaly configured action. flag = keep everything, quarantine = split flagged rows
    :param cfg: Config object to extract relevant variables
    :param df: dataframe to be checked and split if configured
    :return: tuple[DataFrame, DataFrame] with [cleaned_df, anomaly_quarantined_df]
    """
    flag_col = cfg.require("anomaly.flag_col")
    anomaly_reasons_col = cfg.require("anomaly.anomaly_reasons_col")
    if cfg.require("anomaly.action") == "quarantine":
        quarantined_df = df.filter(
            pl.col(flag_col)).with_columns(
                "ANOMALY: " + pl.col(anomaly_reasons_col).alias("reject_reason")
        )

        return df.filter(pl.col(flag_col)), quarantined_df

    # action = 'flag' i.e. keep everything
    return df, df.clear()
