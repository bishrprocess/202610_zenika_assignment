# Assumptions, Decisions and Engineering Judgement

This document records where I made a judgement call while building the HDB resale ETL pipeline, what I considered, and 
why I chose what I did. It is arranged in the order I worked: Bronze ingestion, structure, Silver, Gold, then how the 
design carries to production. The data findings that came out of these decisions are in [`data_profiling_report.md`](data_profiling_report.md).

**Disclaimer.** I built the medallion layer boundaries and the supporting structure myself instead of using a tool that 
generates them, such as OpenMedallion. I want the structural decisions to be visibly my own. 
I also only came across OpenMedallion when the implementation was about half done.

## 1. Principles I applied

| Principle | How it shows up |
|---|---|
| Show the thought process | Every step in the notebook has a markdown explanation, and the reasons for non-obvious choices are written down here |
| Explain trade-offs | Each decision below names the alternative and the cost of what I chose |
| No manual steps | The pipeline starts from the API and ends at the five outputs without any hand-editing |
| No hardcoding | URLs, paths, column names, thresholds, rules and the secret's variable name all come from `config/config.yaml` or the environment |

---

## 2. Decision log

### 2.1 Bronze: Getting the data and storing it

| Decision | Alternatives | Why |
|---|---|---|
| Query the data.gov.sg API instead of downloading files by hand | Manual download | The requirement says to avoid manual interaction. I still save the files locally after querying, so Bronze holds exactly what was received. |
| Key each file by its **dataset id** | Use the display file name | The id is unique and stable. A rename on data.gov.sg cannot create a duplicate copy. |
| Download everything, then restrict scope in Silver | Download only the files covering 2012 to 2017 | The collection spans 1990 onwards, and the Jan 2012 data sits inside a larger file. Scoping in Silver keeps Bronze untouched and makes the scope decision visible and reversible. |
| Retries with exponential backoff, atomic writes, skip files that already exist | Plain one-shot downloads | A flaky network or rate limit should not leave a broken or half-written file, and re-runs should be cheap and safe. |

### 2.2 Structure and configuration

| Decision | Alternatives | Why |
|---|---|---|
| Medallion layers with one folder per layer and one output group per layer | Flat scripts and a single output folder | It maps the required output groups (Raw, Cleaned, Quarantined, Transformed, Hashed) onto a recognised pattern, and later steps can never overwrite earlier data. |
| Notebook as a thin orchestrator over a Python package in `src/` | All logic inside the notebook | The logic stays testable and reusable, and the notebook still documents and shows each step. |
| All variables in a YAML config, including column names | Constants in code | I could not be sure column names would stay stable across the raw files, and reviewers can re-tune behaviour without touching code. |
| Store the id and month of the dataset containing 2012-01 in the config | Hardcode, or detect each run | The id is unlikely to change. It lets the pipeline check that the authoritative set is present and read its schema and column information. |

### 2.3 Silver: combine and tooling

| Decision | Alternatives | Why and trade-off |
|---|---|---|
| **Polars with Parquet** | Spark (PySpark) | The data is just under 1 million rows and about 100 MB. Spark would add a JVM and start-up cost with no benefit at this size. Trade-off: Spark local mode would run unchanged on AWS Glue, which would help in Part 2. I describe Spark/Glue as the scale path instead. |
| Read every column as a string | Let the reader infer types | Type inference can change values. Casting happens later, on copies, so Bronze values are never altered. |
| Jan 2012 file defines the master schema | Union of all columns | The requirement calls Jan 2012 the strict authoritative set. Columns that only exist in other files stay intact in Bronze. |
| Write Parquet with the compression **set explicitly** (zstd) | Rely on the library default | zstd is the default today, but the explicit setting means it can be changed (for faster compression, say) without hunting through code. |
| **ydata-profiling** for the authoritative set | pydeequ | ydata-profiling is quick and produces a good automated report. It needs pandas, which is fine for the small Jan 2012 slice. pydeequ should be reconsidered once the pipeline runs on Spark, so it does not apply yet. |
| Add **Polars-native profiling** for every stage | ydata-profiling at every stage | Importing ydata-profiling is heavy. The Polars metrics are quick to run between stages, so metrics are always at hand. |

### 2.4 Silver: validation and quarantine

| Decision | Alternatives | Why and trade-off |
|---|---|---|
| Validate Town, Flat Type, Flat Model and storey_range against the set of distinct values in Jan 2012 | Maintain a hand-written list | Derived from the data, so it is programmatic and cannot drift from the authoritative set. |
| **Case-sensitive** matching by default | Case-insensitive, whitespace trimmed | "Strict" suggests exact matching. Cost: about half of the flat_model rejections are only casing. It is one setting in the config to change, and the profiling report quantifies the effect. |
| **Quarantine** failing rows instead of deleting them | Drop silently | The brief says failing rows are quarantined. It also keeps every row accounted for. |
| Several reasons per row, joined with `;` | One reason per row | A row that breaks several rules is quarantined once and shows all its problems, so reason counts are honest. |
| Add general quality rules: nulls or blanks, invalid numbers, out-of-scope dates, duplicates | Only the four required columns | The task asks for other appropriate rules. The null check is future-proofing: none were found. Duplicates did turn up (2014 rows). |
| Keep the **first occurrence** of a duplicate | Keep the last, or none | The first record is the original. The choice is configurable. |
| Out-of-scope dates are quarantined, not dropped | Filter them out before validating | Bronze stays untouched, and the quarantine report shows the scope decision. Trade-off: they dominate the bucket (875294 rows), so the report analyses in-scope rejections separately. |

### 2.5 Silver: remaining lease and anomalies

| Decision | Alternatives | Why and trade-off |
|---|---|---|
| Lease starts in **January** of the commencement year (configurable) | Mid-year, or leave blank | The data only has the start year. Trade-off: with the default, the months part is always 0. A configurable start month makes the assumption explicit. |
| Remaining lease rounded **down** to whole months, floored at 0 | Fractional years | Matches the "rounded down to years and months" requirement, and an expired lease never goes negative. |
| Flag anomalies, do not quarantine them | Remove outliers | An unusual price can still be a real sale. Flagging keeps the information without losing data. |
| Heuristic 1: peer-group outliers (IQR or modified z-score, configurable) within town, flat type and year | A single global threshold | Prices differ a lot by town, type and year, so a global rule would flag whole groups. Small groups fall back to year and flat type so thresholds stay stable. |
| Heuristic 2: price per sqm outside configurable bounds | Rely on peers only | It catches errors that peer comparison cannot, such as a group that is wrong as a whole. |
| Recalibrate bounds from **2000 / 7000** to **3000 / 8000** after profiling | Keep my first guess | The profile showed price per sqm between 2375 and 10582, so my initial guess underestimated prices. I changed the bounds after examining the distribution, not before. Trade-off: because they were set from the observed range, they mark its tails and are not an external ground truth. |

### 2.6 Gold: identifier

| Decision | Alternatives | Why |
|---|---|---|
| Follow the specified format exactly (`S` + block digits + average price digits + `YYMM` + town letter) | Any custom scheme | The requirements and examples were clear. |
| Make collisions unique with a **sequence suffix** (`-001`, `-002`, ...) | Drop duplicates, or add a random suffix | The brief forbids dropping records. A sequence is readable and cannot collide with plain identifiers. |
| Make the suffix **deterministic** by ranking within a group on a configurable tie-break order, then on every other column | Row-number order | The same input gives the same identifiers on every run, whatever the order of the rows. |
| Edge cases: a block with no digits becomes `000`; more than 3 digits are cut to 3; the group average is truncated | Fail on such rows | These keep the format fixed-length without rejecting data. Each is documented. |

### 2.7 Gold: de-identification and secret handling

The requirement had three parts: irreversible, uniqueness preserved, and protected against dictionary attacks. The first two were familiar. The third was new to me, because my previous work was on private storage and encrypted enterprise delivery, so I compared the options.

| Option | Irreversible | Unique and consistent | Resists dictionary attacks | Verdict |
|---|---|---|---|---|
| Plain hash (SHA-256 or MD5) | Yes | Yes | **No**: candidates can be hashed and matched | Rejected |
| Per-record random salt + hash | Yes | **No**: the same identifier gives different outputs | Yes | Rejected |
| One global salt + hash | Yes | Yes | **Weak**: the salt is no secret once known | Rejected |
| Encryption (AES) | **No**: reversible with the key | Yes | Yes | Rejected |
| **HMAC-SHA256 with a secret key** | Yes | Yes | Yes | **Chosen** |

I could not find another method that met all three. The identifier space is small and structured, so anyone could enumerate it, which is why the key is what protects the hashes.

| Decision | Why and trade-off |
|---|---|
| Key held in an environment variable, loaded with dotenv when the config is loaded | Never stored in the repository, never printed. It is read only by `get_hash_secret` and fails early if missing or shorter than 32 characters. Trade-off: dotenv puts it in the process environment once the config loads, so anything that dumps the environment would show it. This is the standard pattern, and I accepted it for this scope. |
| Reviewers generate their own key | I would otherwise have to share mine. Trade-off: their hashes will differ from mine. Uniqueness, row counts and format can still be checked. |
| Changing the key changes every hash | Documented in the README. In production this means key versioning has to be planned. |

### 2.8 Verification

| Decision | Why |
|---|---|
| Reconcile row counts after every stage and assert them | Proves nothing is lost: combine loses nothing, every row is cleaned or quarantined, and Gold keeps every cleaned row. |
| Assert identifier uniqueness and format, and hash uniqueness and format | Turns the requirements into checks that run every time. |

---

## 3. Assumptions register

| # | Assumption | Impact if wrong |
|---|---|---|
| A1 | The scope Jan 2012 to Dec 2017 is inclusive at both ends | Rows at the edges move between Cleaned and Quarantined |
| A2 | The lease starts in January of the commencement year | Months of remaining lease would not always be 0 |
| A3 | HDB leases are 99 years and the as-of date is 1 Jan 2027 | Taken from the requirements |
| A4 | "Strict authoritative set" means exact, case-sensitive matching | A large number of probable valid rows are quarantined. Switch with config |
| A5 | A duplicate is a row identical in every column except `source_file`; the first is kept | Different keys would change which rows are quarantined |
| A6 | Price anomalies are flagged, not removed | Nothing is removed if the rule is too loose |
| A7 | The identifier's average price is computed over all validated rows, including flagged anomalies | Outliers could shift the price digits slightly |
| A8 | The first letter of the town is taken in upper case | Only matters if towns are not stored in capitals |
| A9 | Dataset ids are stable on data.gov.sg | A changed id would need a config update |
| A10 | Column names could change over time | Why names live in config |
| A11 | Each run uses its own secret, so hashes are not comparable between keys | The same record hashes differently under another key |

## 4. Known limitations and next steps

- **Strictness has a cost.** I would agree an explicit allowed-value list with the data owner and standardise casing at the source.
- **Quarantine mixes scope and quality.** The report separates them, but a cleaner design might route out-of-scope rows to their own bucket.
- **Anomaly bounds are static.** Year-specific or percentile-based bounds would adapt to price changes.
- **Duplicates have no root cause.** I would check for overlapping source files or an upstream issue.
- **Key rotation.** Changing the secret changes every hash. A key version stored alongside the output would make rotation manageable.
- **Single machine.** Polars in memory is right for this volume. Growth in the data is the trigger to move to Spark.

## 5. From notebook to production

The Part 1 choices were made with the AWS target in mind. The table shows how each one carries over. The full 
design is in [`../prod_architecture`](../prod_architecture/), and this table is indicative.

| Concern | Part 1 choice | Production counterpart |
|---|---|---|
| Layers | One folder per medallion layer | One S3 prefix or bucket per layer, with Bronze treated as immutable |
| Compute | Polars, in memory | Spark on AWS Glue. The logic is written as column expressions and filters, which translate to Spark. Spark local mode code runs unchanged on Glue |
| Data quality | Polars rules plus ydata-profiling and per-stage metrics | Deequ (pydeequ) once on Spark |
| Large files | Streamed to disk in chunks, written atomically | Streamed to S3 so memory use does not grow with file size, which matters for files over 100 MB |
| Reliability | Retries with backoff, skip finished files, idempotent re-runs | The same behaviour under an orchestrator, so a failed run can simply be repeated |
| Configuration | One YAML file | A versioned config outside the code |
| Secret | Environment variable | A secrets manager, injected through the job's role, with key versions |
| Consumption | Parquet outputs | The Hashed output as the table the Data Science Team queries, with private connectivity to Tableau |
