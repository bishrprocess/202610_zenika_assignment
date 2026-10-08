"""
Gold layer: de-identify the resale identifier created in identifier.py
According to requirements, algorithm needs to do the following:
- be irreversible
- preserve uniqueness
- protect against dictionary attacks

Algorithm: HMAC-SHA256, keyed with a secret (referred to as the "pepper"), with an output of 64 Hex characters

Secret is stored in environment variable 'deidentify.secret_env_var' (see .env.example).
Never stored in the repo or the config. Never to be logged.
Secret needs to be kept stable as changing it will change the hashes. To be kept in a secret manager.
Downstream consumers will only work with hashed-output based on requirements given.
"""
from __future__ import annotations

import hashlib
import hmac
import logging
from pathlib import Path

import polars as pl

from src.config import Config, get_hash_secret

logger = logging.getLogger(__name__)


def hmac_sha256_hex(
        values: list[str | None],
        secret: str
) -> list[str]:
    """
    Use HMAC-SHA256 algorithm to calculate hash for each value with the secret key.
    :param values: strings to hash (nulls are rejected as all nulls would hash to the same value - there are no nulls
    in identifier column)
    :param secret: secret key from secret env var, if not set consult above description of file or readme
    :return: list of 64 character lowercase hex character hashes in the same order as values.
    """
    # Build keyed state to copy per value
    key_template = hmac.new(secret.encode("utf-8"), digestmod=hashlib.sha256)

    digests = []
    for value in values:
        # While it is highly unlikely a null has escaped to this point, this will protect against faulty runs
        if value is None:
            raise ValueError("Cannot hash a null identifier value, please check input data values.")

        keyed = key_template.copy()
        keyed.update(value.encode("utf-8"))
        digests.append(keyed.hexdigest())

    return digests


def deidentify_identifier(
        cfg: Config,
        transformed_df: pl.DataFrame,
        secret: str | None = None,
) -> pl.DataFrame:
    """
    Replace plain resale identifier with its keyed hash from hmac_sha256_hex()
    :param cfg: Config object to extract relevant variables.
    :param transformed_df: dataframe containing the identifier column with uniqueness.
    :param secret:  hashing secret; read from the configured env variable when not directly given to function.
    :return:  dataframe with hashed identifier column, plain column is dropped when deidentify.drop_plain_identifier
    is set to True.
    """
    if cfg.require("deidentify.algorithm") != "hmac_sha256":
        raise ValueError("Only deidentify.alogirth 'hmac_sha256' is supported.")

    if cfg.get("deidentify.digest", "hex") != "hex":
        raise ValueError("Only deidentify.digest 'hex' is supported.")

    id_col = cfg.require("identifier.resale_id_col")
    hash_id_col = cfg.require("deidentify.hash_id_col")

    if id_col not in transformed_df.columns:
        raise ValueError("Identifier column {col} not found in input dataframe.".format(col=id_col))

    secret = secret or get_hash_secret(cfg=cfg)

    identifiers_data = transformed_df[id_col]
    hashed_identifiers_data = pl.Series(hash_id_col, hmac_sha256_hex(
        values=identifiers_data.to_list(),
        secret=secret)
    )

    # Check for equal unique count
    if hashed_identifiers_data.n_unique() != identifiers_data.n_unique():
        raise RuntimeError("Hash collision detected due to unequal unique count; uniqueness was not preserved.")

    output_df = transformed_df.with_columns(
        hashed_identifiers_data
    )
    # Drop plain identifier column
    if cfg.get("deidentify.drop_plain_identifier", True) and hash_id_col != id_col:
        output_df = output_df.drop(id_col)

    assert output_df.height == transformed_df.height, "Row count drift occurred"
    logger.info("De-identified %d identifiers with HMAC-SHA256", output_df.height)

    return output_df


def write_hashed_output_df(
        cfg: Config,
        hashed_df: pl.DataFrame,
) -> Path:
    """
    Write the hashed output dataframe as parquet file.
    :param cfg: Config object to extract relevant variables.
    :param hashed_df: dataframe containing the hashed identifier column
    :return: path written to
    """
    hashed_df_path = cfg.path("gold_hashed", create=True) / "{filename}.parquet".format(
        filename=cfg.require("outputs.gold_hashed_filename")
    )
    hashed_df.write_parquet(hashed_df_path, compression="zstd")
    logger.info("Wrote hashed output -> %s", hashed_df_path)

    return hashed_df_path