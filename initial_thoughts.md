Need to demonstrate clear thought process and engineering decisions at every step  
Trade-offs should be explained - reason behind choices  
AVOID Manual interactions and Hardcoding

### Part 1 - Pre-Ingestion

- data is on [data.gov.sg](https://data.gov.sg/collections/189/view) (direct link)
- considering earlier requirements need to query with API and not download and save
- nevertheless, I can save the files locally after querying with API
- Looking at the data available, it seems to go from 1990 to 2017 onwards?
- for this assignment, we need only 2012-01 to 2017-12 (Inclusive)
- datasets are uniquely identifiable on data.gov.sg using their dataset_id
  - using the dataset id for filename to avoid duplicated copies in case of name change
- medallion architecture for table handling

### Part 1 - Post initial Ingestion

- Now that I have access to data samples and have assured that I can pull the data I can focus on repo structure
  - Repo structure should support maintainability and configurability (avoiding hard-coding of values)
  - Design and implement a structure that supports the intended medallion architecture and notebook based pipeline
  - Implement config variable management for easier maintainability and configurability
  - Add retries, backoff and clearer API query flow for easier readability and debugging.
- Now that I have restructured the repository and ingestion flow, I can move on to data combination and quality
- Since the dataset id is unlikely to change, I will add the id of the dataset containing 2012-01 to the config
  - This way I can build a check ensuring that the authoritative set is available
  - Additionally, I can use it to extract the necessary column and schema information
- Data handling library choice: While Spark (pyspark) would be the most reasonable choice for larger data.
  - Additionally, Spark local mode code would run unchanged on AWS glue which is an advantage for Part 2 architecture.
  - However, a polars + parquet implementation would be quicker and better suited for smaller data whilst sparing the
  spark related setup and dependencies at an early stage.
  - After quickly reading and analyzing the raw data, the total row count is a little bit above 1 million rows
    - The row count does not warrant a spark implementation at the moment
- For data profiling using an open source framework, I am deciding between ydata-profiling and deequ (pydeequ).
  - ydata is quick and provides good automated profile reports
  - deequ scales better for larger data sets and integrates well with spark