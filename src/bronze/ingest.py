"""
Bronze Layer: Download raw HDB resale datasets from data.gov.sg and do not modify them.

"""
from __future__ import annotations

import requests
from typing import Iterable, List
import os
from pathlib import Path
import logging
import time

from src.config import Config, load_config

logger = logging.getLogger(__name__)

RETRY_STATUS = [429, 500, 502, 503, 504]


def _get_with_retry(
        session: requests.Session,
        url: str,
        *,
        timeout: int,
        max_retries: int,
        backoff: float,
        stream: bool = False,
) -> requests.Response:
    """
    GET request with exponential backoff on network errors listed in [RETRY_STATUS]
    """
    # Range 1 to max_retries + 1 for cleaner debugging
    for attempt in range(1, max_retries + 1):
        try:
            resp = session.get(url, timeout=timeout, stream=stream)
            # Check whether response status code in [RETRY_STATUS]
            if resp.status_code in RETRY_STATUS:
                resp.close()
                raise requests.HTTPError("Retryable status {}".format(resp.status_code))
            resp.raise_for_status()
            return resp
        except requests.RequestException as e:
            if attempt == max_retries:
                raise
            else:
                # Backoff exponential leeway in case of high pressure on systems
                wait = backoff * 2 ** (attempt - 1)
                logger.warning("Attempt %d/%d failed (%s). Retrying in %fs",
                               attempt, max_retries, e, wait)
                time.sleep(wait)


def get_data_gov_collection_dataset_list(
        cfg: Config,
        session: requests.Session
    ) -> List[str]:
    """
    Extract necessary variables from Config to get intended dataset ids for download.

    :param cfg: Config object with variables and helpers
    :param session:
    :return: Return the child dataset ids of the defined collection in config.
    """
    # Get configured collection id and metadata url GET
    collection_id = cfg.require("source.collection_id")
    data_gov_get_collection_url = cfg.require("source.collection_metadata_url").format(
        collection_id=collection_id)

    resp = _get_with_retry(
        session=session,
        url=data_gov_get_collection_url,
        timeout=cfg.require("source.request_timeout_seconds"),
        max_retries=cfg.require("source.max_retries"),
        backoff=cfg.require("source.retry_backoff_seconds"),
    )

    # Parse resp into JSON
    payload = resp.json()
    logger.debug("Collection payload: %s", payload)

    if not payload:
        raise ValueError("Empty response for collection {collection_id}".format(collection_id=collection_id))

    #
    metadata = payload.get("data", {}).get("collectionMetadata", {})

    # If response is not empty, check whether collectionId matches collection_id
    if str(metadata.get("collectionId")) != str(collection_id):
        logger.error("Collection id mismatch for %s", collection_id)
        raise ValueError("Collection id mismatch for %s", collection_id)

    child_dataset_ids = metadata.get("childDatasets") or []

    # During testing, sample was a list but in case that changes to an object
    ids = [c["datasetId"] if isinstance(c, dict) else str(c) for c in child_dataset_ids]

    return ids


def download_data_gov_dataset(
        cfg: Config,
        session: requests.Session,
        dataset_id: str,
        destination_path: str,
        overwrite: bool = False,
) -> tuple[Path, bool]:
    """
    Downloads ONE dataset.
    :return: (path, downloaded_now)
    """
    timeout = cfg.require("source.request_timeout_seconds")
    max_retries = cfg.require("source.max_retries")
    backoff = cfg.require("source.retry_backoff_seconds")

    # Re-use existing file for dataset to ensure idempotency
    existing = list(destination_path.glob("{dataset_id}".format(dataset_id=dataset_id)))

    # If file exists and overwrite option not set to True, operation is skipped
    if existing and not overwrite:
        logger.info("Skipping %s (already exists: %s)", dataset_id, existing[0].name)
        return existing[0], False

    download_init_url = cfg.require("source.initiate_download_url").format(dataset_id=dataset_id)

    init_resp = _get_with_retry(
        session=session,
        url=download_init_url,
        timeout=timeout,
        max_retries=max_retries,
        backoff=backoff,
    )

    # Extract actual download url from init_resp
    download_url = init_resp.json()["data"]["url"]

    dest_path = destination_path / "{dataset_id}.csv".format(dataset_id=dataset_id)
    # Avoid truncated files when downloading in case of incomplete download
    tmp = dest_path.with_suffix(dest_path.suffix + ".part")

    with _get_with_retry(
        session=session,
        url=download_url,
        timeout=cfg.require("source.download_timeout_seconds"),
        max_retries=max_retries,
        backoff=backoff,
        stream=True
    ) as resp:
        with tmp.open("wb") as f:
            for chunk in resp.iter_content(chunk_size=cfg.require("source.chunk_size_bytes")):
                f.write(chunk)

    tmp.replace(dest_path)
    logger.info("Saved %s -> %s", dataset_id, dest_path)
    return dest_path, True


def run_ingestion_pipeline(
        cfg: Config = None,
        dataset_ids: Iterable[str] = None,
        overwrite: bool = False,
) -> dict[str, list]:
    """
    Download all datasets from the configured collection into the Bronze raw folder.

    :param cfg: Config object with variables and helpers
    :param dataset_ids: List of dataset ids to ingest
    :param overwrite: Boolean to determine whether to overwrite existing datasets
    :return: Returns a dictionary [str,list] with the following keys:
                - downloaded: Download successful to destination folder
                - skipped: Download skipped, look at logs for more information
                - failed : Download failed, look at logs for more information
    """
    # Get config to extract relevant variables
    cfg = cfg or load_config()
    dest_path = cfg.path("bronze_raw", create=True)
    delay_seconds = cfg.get("source.rate_limit_delay_seconds", 5)

    # Create result dictionary structure to clearly identify what occurred with each dataset
    result: dict[str, list] = {"downloaded": [], "skipped": [], "failed": []}

    with requests.Session() as session:
        ids = list(dataset_ids or get_data_gov_collection_dataset_list(cfg=cfg, session=session))
        logger.info("Found %d dataset(s) to ingest", len(ids))

        for dataset_id in ids:
            try:
                path, downloaded = download_data_gov_dataset(
                    cfg=cfg,
                    session=session,
                    dataset_id=dataset_id,
                    destination_path=dest_path,
                    overwrite=overwrite
                )
                # Append download status and respective dataset path
                result["downloaded" if downloaded else "skipped"].append(dataset_id)

                # If download was successful, delay to avoid rate limit
                if downloaded:
                    time.sleep(delay_seconds)
            except (requests.RequestException, KeyError, ValueError, OSError) as e:
                logger.error("Download failed for %s: %s", dataset_id, e)
                result["failed"].append(dataset_id)

        return result


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    print(run_ingestion_pipeline())