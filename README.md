# HDB Resale Flat Prices: ETL Pipeline (Jan 2012 to Dec 2017)

A medallion-architecture (Bronze / Silver / Gold) ETL pipeline that downloads HDB resale flat price 
data from [data.gov.sg](https://data.gov.sg/collections/189/view), validates and cleans it, 
adds a unique **Resale Identifier**, and produces a **hashed** version of that identifier.

The pipeline runs end to end from one Jupyter notebook, [`notebooks/hdb_resale_etl.ipynb`](notebooks/hdb_resale_etl.ipynb).
The notebook is a thin orchestrator: it explains each step in Markdown, calls the code in `src/`, 
shows the result, and checks it with assertions. The saved cell outputs show a complete run.

**Contents**
1. [Overview](#overview)
2. [Requirement map](#requirement-map)
3. [Quick start](#quick-start)
4. [Configuration and secrets](#configuration-and-secrets)
5. [Architecture](#architecture)
6. [Pipeline steps and rules](#pipeline-steps-and-rules)
7. [Outputs](#outputs)
8. [Design decisions and trade-offs](#design-decisions-and-trade-offs)
9. [Troubleshooting](#troubleshooting)
10. [Repository hygiene](#repository-hygiene)

## Overview

| Medallion layer | Output group | What it holds |
|---|---|---|
| **Bronze** | **Raw** | Files downloaded from data.gov.sg exactly as received. Never modified. |
| **Silver** | **Cleaned** | Combined, validated rows with remaining lease and price-anomaly flags |
| **Silver** | **Quarantined** | Rows that failed any rule, each with a `reject_reason` |
| **Gold** | **Transformed** | Cleaned rows plus the unique `resale_identifier` |
| **Gold** | **Hashed** | Same rows with the identifier replaced by a keyed hash (HMAC-SHA256) |

Key properties:

- **Nothing is dropped silently.** The notebook asserts that every combined row is either cleaned or quarantined, and that 
Gold keeps every cleaned row.
- **Raw data is never modified.** All cleaning happens in Silver, so rows that fail a rule are quarantined with a reason
instead of being deleted.
- **Configuration.** URLs, paths, thresholds, rules and column names are kept in `config/config.yaml` and not hard-coded
as much as possible.
- **Deterministic.** Re-running produces the same identifiers regardless of input row order.

## Requirement map

| Requirement | Where it is implemented | Notebook section |
|---|---|---|
| Extract the collection as-is | `src/bronze/ingest.py` | 2 |
| Combine into one master dataset, Jan 2012 as authoritative set | `src/silver/combine.py` | 3.1 |
| Data profiling | `src/silver/profiling.py`, `reports/data_profiling_report.md` | 3.2, 5 |
| Validate Town, Flat Type, Flat Model, storey_range and quarantine failures | `src/silver/validation.py` | 3.3 |
| Remaining lease as of 1 Jan 2027 | `src/silver/lease.py` | 3.4 |
| Anomalous resale prices | `src/silver/anomaly.py` | 3.5 |
| Additional cleaning and validation rules | `src/silver/validation.py` (missing values, scope, numeric, duplicates) | 3.3 |
| Insights and assumptions | `reports/assumptions_and_insights.md` | n/a |
| Resale Identifier, unique, no dropped records | `src/gold/identifier.py` | 4.1 |
| Irreversible de-identification | `src/gold/deidentify.py` | 4.2 |
| Five output groups | written in sections 3.6, 4.1 and 4.2, listed in section 5 | 3.6, 3, 5 |
| Production Architecture (Part 2) | `prod_architecture` | n/a |

## Quick start

**Prerequisites:** Python 3.10 or later, Git, and internet access to `data.gov.sg` and PyPI.

**1. Clone and create an environment**

```bash
git clone <repo-url>
cd <repo-folder>

python -m venv .venv
source .venv/bin/activate          # Windows PowerShell: .venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

**2. Create your hashing secret** (required; see [Configuration and secrets](#configuration-and-secrets))

```bash
cp .env.example .env               # Windows: copy .env.example .env
python -c "import secrets; print(secrets.token_hex(32))"
```

Open `.env` and replace `CHANGE_ME` with the printed value (no quotes, no spaces around `=`):

```
HASH_PEPPER=<the 64-character value you just generated>
```

**3. Run the notebook**

```bash
jupyter lab notebooks/hdb_resale_etl.ipynb
```

Select the kernel from the `.venv` environment, then choose **Run > Run All Cells** (or **Kernel > Restart & Run All**).
It should finish in a few minutes, mostly spent downloading files politely/honourably (there is a delay between downloads to 
respect the site's rate limits).

What to expect:

- Setup checks that the secret is present and long enough, and stops immediately if not.
- Ingestion downloads each dataset in the collection into `data_storage/bronze/raw/`. Files already present are skipped,
so re-runs are quick.
- Each later section prints a summary table and runs assertions. If any assertion fails, the run stops with a message 
saying what was violated.
- The last sections reconcile row counts across stages and list every output file with its size and row count.

## Configuration and secrets

All behavior is controlled by [`config/config.yaml`](config/config.yaml):

| Section | Controls |
|---|---|
| `project` | name, log level|
| `source` | collection id, API URLs, timeouts, retries, backoff, rate-limit delay, optional include/exclude dataset ids |
| `paths` | folder for each layer (relative to the repository root) |
| `outputs` | output file names |
| `scope` | month range in scope (Jan 2012 to Dec 2017) and the month column |
| `reference` | authoritative month (Jan 2012) that defines the schema and allowed values |
| `validation` | validated columns, required columns, numeric minimums, duplicate handling, matching strictness, reject-reason column |
| `calc_remaining_lease` | lease term (99 years), as-of date (2027-01-01), output column names |
| `anomaly` | method, grouping, thresholds, price-per-sqm bounds, action (`flag` or `quarantine`) |
| `identifier` | identifier format and uniqueness settings |
| `deidentify` | hashing algorithm, digest, secret variable name, output column |

Paths are resolved from the repository root, so the notebook works on any machine.

### The hashing secret (`HASH_PEPPER`)

The identifier is hashed with HMAC-SHA256 using a secret key (a "pepper"). 
The secret is **never stored in the repository, the config or the notebook output.**

- It is read from the environment variable named in `deidentify.secret_env_var` (default `HASH_PEPPER`) and must be at 
least 32 characters. The pipeline refuses to run without a valid one.
- `.env.example` is a committed template with a deliberately invalid placeholder. `.env` holds your real value 
and is git-ignored.
- **Reviewers should generate their own secret.** Hashes will therefore differ from the ones committed with this 
repository. That is expected: the committed hashes were produced with the author's secret, which is not shared, so no 
one can reproduce or reverse them (as interpreted from the requirements). Uniqueness, row counts and the 64-character 
hex format can still be verified, and the notebook asserts all three.
- If you change `.env` while the notebook kernel is running, restart the kernel. An environment variable that is already
set is not overridden by `.env`.

## Architecture

```
data.gov.sg collection 189
        |  download as-is (retry, backoff, atomic writes, skip existing)
        v
BRONZE   data_storage/bronze/raw/                 Raw
        |  read all columns as strings, stack, schema from Jan 2012 file
        v
SILVER   data_storage/silver/combined/            master dataset (intermediate)
        |  profile Jan 2012 -> build allowed values -> apply all rules together
        |-----------------------------> data_storage/silver/quarantined/    Quarantined
        v
         add remaining lease -> flag price anomalies
         data_storage/silver/cleaned/             Cleaned
        |  add unique Resale Identifier
        v
GOLD     data_storage/gold/transformed/           Transformed
        |  HMAC-SHA256 with secret from environment
         data_storage/gold/hashed/                Hashed
```

Repository layout:

```
.
├── README.md
├── git_summary/
│   └── requirements.txt
├── .env.example                       template for the secret; copy to .env
├── .gitignore
├── config/
│   └── config.yaml
├── notebooks/
│   └── hdb_resale_etl.ipynb           main deliverable: runs everything, outputs saved
├── src/
│   ├── config.py                      loads config.yaml, resolves paths, reads the secret
│   ├── logging_setup.py               log level from config
│   ├── bronze/ingest.py               download raw files
│   ├── silver/
│   │   ├── combine.py                 combine files into the master dataset
│   │   ├── profiling.py               ydata-profiling report and per-stage metrics
│   │   ├── validation.py              authoritative-set and quality rules, quarantine
│   │   ├── calc_remaining_lease.py    calculate remaining lease
│   │   └── anomaly_resale_prices.py   price-anomaly flags
│   └── gold/
│       ├── identifier.py              Resale Identifier
│       └── deidentify.py              keyed hash
├── data_storage/
│   ├── bronze/raw/                    Raw
│   ├── silver/combined/               intermediate combined dataset
│   ├── silver/cleaned/                Cleaned
│   ├── silver/quarantined/            Quarantined
│   ├── gold/transformed/              Transformed
│   └── gold/hashed/                   Hashed
├── reports/
│   ├── data_profiling_report.md       insights across the pipeline and the quarantine bucket
│   ├── assumptions_and_insights.md
│   └── profiling/                     ydata-profiling HTML/JSON, per-stage metrics, reference value sets
└── architecture/                      Part 2
```

## Pipeline steps and rules

### Bronze: ingestion (`bronze/ingest.py`)

- Child datasets are discovered from the collection metadata at run time, not hardcoded. Optional `include_dataset_ids` 
/ `exclude_dataset_ids` in the config narrow the list.
- Each file is downloaded unchanged. Failed requests are retried with exponential backoff on network errors, HTTP 429 
and 5xx.
- Files are written to a temporary `.part` file and renamed when complete, so a failed download never leaves a truncated
file that looks valid.
- Existing datasets are skipped, which makes re-runs idempotent.

### Silver: combine (`silver/combine.py`)

- Every file is read **as strings** (no type inference), so values stay exactly as received.
- The file containing the authoritative month (Jan 2012) defines the main schema (columns and order). Columns that exist
only in later files, such as `remaining_lease`, are left out of the combined dataset and remain intact in Bronze. 
Remaining lease is recomputed in a later step as required.
- A `source_file` column records where each row came from.
- Nothing is filtered or de-duplicated here, so every problem row can be quarantined with a reason in the validation step.

### Silver: profiling (`silver/profiling.py`)

- **Authoritative set:** [ydata-profiling](https://github.com/ydataai/ydata-profiling) profiles the Jan 2012 rows. 
ydata-profiling does not read Polars natively, so only this small slice is converted to pandas (numeric columns are cast
on that copy only). Reports are written to `reports/profiling/`.
- **Every stage:** a lightweight Polars-native profile (rows, nulls, distinct values per column) is taken at each stage
and saved as `reports/profiling/{stage}_profile.csv`. It feeds the lifecycle section of `reports/data_profiling_report.md`.

### Silver: validation and quarantine (`silver/validation.py`)

All rules run together on the combined data. A row that breaks several rules is quarantined once, with every reason in
`reject_reason` (joined by `;`). Values are never modified.

| Rule | Reject reason |
|---|---|
| A required column is null or blank | `MISSING_REQUIRED` |
| Month outside the configured scope (Jan 2012 to Dec 2017) | `OUT_OF_SCOPE_DATE` |
| Price, area or lease year not numeric, below its configured minimum, or lease starting after the sale year | `INVALID_NUMERIC` |
| Repeat of an earlier row (all columns except `source_file`); the first occurrence is kept | `DUPLICATE` |
| `town`, `flat_type`, `flat_model` or `storey_range` not among the values found in Jan 2012 (nulls fail too) | `INVALID_TOWN`, `INVALID_FLAT_TYPE`, `INVALID_FLAT_MODEL`, `INVALID_STOREY_RANGE` |

The allowed values are taken from the Jan 2012 rows of the full combined data, before any row is removed, and are saved 
as JSON (`reports/profiling/reference_values.json`) for traceability. Case and whitespace handling when comparing is set
by `validation.case_sensitive` and `validation.strip_whitespace`.

### Silver: remaining lease (`silver/lease.py`)

The data only gives the lease start **year**, so the lease is assumed to start in January of that year and run 99 years.
Remaining lease is measured from 1 Jan 2027, rounded down to whole months, and split into `remaining_lease_years`, 
`remaining_lease_months` and a text column such as `52 years 00 months`. Because the start month is January, the months
part is always 0. Expired leases are set to 0.

### Silver: price anomalies (`silver/anomaly.py`)

Rows are **flagged, not removed** by default (`anomaly.action: flag`), because an unusual price can still be a real
sale. Set the action to `quarantine` to move them out instead.

1. **Peer-group outliers.** The price is compared with flats of the same town, flat type and year, using the IQR rule 
(a median/MAD option is also available). Groups smaller than `min_group_size` fall back to flat type and year so 
thresholds stay stable.
2. **Price per sqm outside set plausible bounds.** This catches errors that peer group outliers miss, such as a whole group being wrong.

Added columns: `resale_price_num`, `resale_price_per_sqm`, `year`, `is_price_anomaly`, `anomaly_reason` (for example `PRICE_HIGH_FOR_PEERS`).

### Gold: Resale Identifier (`gold/identifier.py`)

Format: `S` + `BBB` + `PPP` + `YYMM` + `T` (12 characters).

| Part | Rule | Example |
|---|---|---|
| `S` | constant prefix | `S` |
| `BBB` | first 3 digits of `block` after removing non-digits, left-padded with zeros | block `19` gives `019` |
| `PPP` | first 3 digits of the average resale price of the (year-month, town, flat type) group | $230,000 gives `230` |
| `YYMM` | year-month of the row | `2012-01` gives `1201` |
| `T` | first letter of the town | Ang Mo Kio gives `A` |

Example: block 19, Ang Mo Kio, 2012-01, group average $230,000 gives `S0192301201A`.

**Uniqueness without dropping records.** Many rows legitimately share the same identifier. Rows that collide get a 
sequence suffix (`S0192301201A-001`, `-002`, ...). Rows are ranked inside each colliding group by a configured tie-break
order (month, town, flat_type) and then by every remaining column, so the ranking is a total order. The result is 
deterministic and independent of input row order. Rows that were already unique keep the plain 12-character form, and a
suffixed identifier can never equal a plain one.

Edge cases: a block with no digits becomes `000`; a block with more than 3 digits keeps its first 3; 
the group average is truncated, not rounded, and is computed over all validated rows (including any flagged price anomalies).

### Gold: de-identification (`gold/deidentify.py`)

The identifier is replaced by **HMAC-SHA256** (64 hex characters) keyed with the secret from `HASH_PEPPER`:

- **Irreversible:** HMAC-SHA256 is a one-way function.
- **Preserves uniqueness:** it is deterministic and collision-resistant, and the code checks that the number of distinct
hashes equals the number of distinct identifiers.
- **Protects against dictionary attacks:** the identifier space is small and structured, so a plain or publicly salted 
hash could be brute-forced by hashing every candidate. Without the secret key an attacker cannot do that.
A random per-row salt would also stop dictionaries but would break uniqueness by value and cross-run consistency, so it is not used.

The Hashed output drops the plain identifier (`deidentify.drop_plain_identifier`).

## Outputs

All outputs are Parquet (zstd). Folders come from `paths.*` and file names from `outputs.*` in the config.

| Output group | Folder | Contents |
|---|---|---|
| Raw | `data_storage/bronze/raw/` | Source CSVs as downloaded |
| Cleaned | `data_storage/silver/cleaned/` | Validated rows plus remaining-lease and anomaly columns |
| Quarantined | `data_storage/silver/quarantined/` | Rejected rows plus `reject_reason` |
| Transformed | `data_storage/gold/transformed/` | Cleaned rows plus `resale_identifier` |
| Hashed | `data_storage/gold/hashed/` | Cleaned rows plus the hashed identifier (`resale_identifier_hashed`) instead of the plain one |

Intermediate and supporting files: `data_storage/silver/combined/` (master dataset), `reports/profiling/` 
(profiling reports, `stage_profile.csv`, `reference_values.json`).

The repository ships with the results of the author's run (notebook cell outputs and the `data_storage/` folders) 
so they can be inspected without executing anything. 
Re-running regenerates them; Hashed values will differ because each run uses its own secret.

**Checks built into the notebook:** row counts reconcile across stages; every identifier is unique and matches the 
specified format; every hash is unique and 64 hex characters; the plain identifier is absent from the Hashed output.

## Design decisions and trade-offs

The full list is in [`reports/assumptions_and_insights.md`](reports/assumptions_and_insights.md). The main ones:

- **Polars with Parquet instead of Spark.** The notebook prints the size of each file and of the combined data. It fits 
comfortably in memory, so Spark would add a JVM dependency and start-up cost without a benefit at this size. Spark or 
AWS Glue is the scale path described in the Part 2 architecture.
- **ydata-profiling plus Polars-native metrics instead of pydeequ.** ydata-profiling gives a descriptive report of the 
authoritative set. Per-stage metrics are a few lines of Polars. Row-level quarantine is clearer as filters than as 
Deequ constraints, and pydeequ needs a Spark-version-matched JAR that makes setup fragile for reviewers.
- **Out-of-range rows are quarantined, not deleted.** Raw must stay untouched, and some source files extend beyond 
Dec 2017 or before Jan 2012, so such rows are quarantined as `OUT_OF_SCOPE_DATE`. This reason can account for a large 
share of the quarantine bucket; the profiling report separates it from genuine data-quality failures.
- **Strict authoritative set.** Town, flat type, flat model and storey range are compared with the Jan 2012 values. 
Values that first appear in later years are quarantined, which is expected with strict validation.
- **Duplicates.** A repeat of an earlier row (all columns except `source_file`) is quarantined and the first occurrence is kept.
- **Anomalies are flagged, not removed.** A high or low price can be genuine.
- **Remaining lease.** Only the lease start year is available, so a January start is assumed, therefore the months part is always 0.
- **Secret handling (`.env`).** `load_config()` loads `.env` into the process environment, so the secret is available to
the whole process once the config is loaded, although only the de-identification step reads it and it is never logged 
- or stored in the config object. Of course, real environment variables take precedence over `.env`, 
so in production the secret would come from a secrets manager and no `.env` file would exist. 
Reading `.env` lazily at the point of use was considered and rejected as unnecessary for this scope.
- **Log safety.** Logging is configured from the config.

## Troubleshooting

| Symptom | Fix |
|---|---|
| `Environment variable 'HASH_PEPPER' is not set` | Create `.env` from `.env.example` (or export the variable), then restart the kernel. |
| `'HASH_PEPPER' must be at least 32 characters long` | You are still using the `CHANGE_ME` placeholder. Generate a real value (see Quick start). |
| `ModuleNotFoundError: src` | Open the notebook from inside the repository and select the `.venv` kernel. The first cell finds the repository root from `config/config.yaml` and adds `src/` to the path. |
| `Missing required config key: ...` | Compare your `config/config.yaml` with the one in the repository; a key was renamed or removed. |
| Downloads fail with HTTP 429 or time out | The code already retries with backoff. Re-run the ingestion cell (finished files are skipped), or increase `source.rate_limit_delay_seconds` and `source.max_retries`. |
| No access to data.gov.sg from your network | Skip the ingestion cell and place the CSV files in `data_storage/bronze/raw/`. Every later step reads from that folder. |
| Profiling is slow or ydata-profiling will not install | Use a fresh virtual environment (it pins numpy and pandas ranges), or pass `minimal=True` to `profile_reference`. |
| Want a clean rebuild | Delete the generated folders under `data_storage/` (except `data_storage/bronze/raw/` if you want to keep the downloads) and run all cells again. |

## Repository hygiene

- `.env` is git-ignored; only `.env.example` is committed. No secrets are stored in the repository.
- Raw input files are included under `data_storage/bronze/raw/`, as the submission requires.
