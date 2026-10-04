# Youtube-Trending_Analysis-AWS_Pipeline


A serverless data lake pipeline that ingests raw YouTube trending video data (CSV statistics and JSON category reference files), cleans and converts it to Parquet, joins it into an analytics table, and serves it to Amazon Athena and Amazon QuickSight.

Master's coursework project, University of Massachusetts Dartmouth, 2025. Built by a team of two: Jayanth Mekala and Rishindra Chowdhary Maddineni.

## Architecture

```
Raw files (CSV + JSON)
        |
        v
S3 raw bucket  -->  Glue crawler  -->  Glue Data Catalog (project-youtube-raw)
        |                                      |
        | S3 event                             | Glue PySpark job
        v                                      v
Lambda (JSON -> Parquet)              CSV -> Parquet, partitioned by region
        |                                      |
        +-----------> S3 cleansed bucket <-----+
                            |
                            v
              Glue ETL job: join statistics with categories
                            |
                            v
        S3 analytics bucket (Parquet, Snappy, partitioned by region and category_id)
                            |
                            v
                 Athena SQL  -->  QuickSight dashboards
```

## How it works

| Stage | Service | What happens |
|---|---|---|
| Ingestion | Amazon S3 | Raw CSV and JSON files land in a raw bucket, organised by region |
| Cataloging | AWS Glue crawler | Detects schemas and registers tables in the Glue Data Catalog |
| JSON cleansing | AWS Lambda | Triggered by S3 uploads; flattens the category JSON with pandas, writes Parquet with PyArrow, and creates or updates the Glue table |
| CSV cleansing | AWS Glue (PySpark) | Reads the raw statistics with a pushdown predicate, applies an explicit schema mapping, and writes Parquet partitioned by region |
| Analytics build | AWS Glue ETL | Joins statistics with category reference data and writes a Snappy-compressed table partitioned by region and category |
| Query | Amazon Athena | SQL over the analytics table, including validation queries for nulls and duplicates |
| Reporting | Amazon QuickSight | Dashboards for top categories, regional popularity and engagement |
| Security | AWS IAM | Service roles scoped to the buckets and Glue resources each job needs |

## Repository structure

```
.
├── lambda/
│   └── lambda_function.py                    # S3-triggered JSON -> Parquet + Glue table registration
├── glue/
│   ├── csv_to_parquet.py                     # Raw CSV -> cleansed Parquet, partitioned by region
│   └── youtube_analytics_etl.py              # Join into the final analytics table
├── docs/
│   └── project_report.pdf                    # Full write-up
└── README.md
```

## Dataset

[Trending YouTube Video Statistics](https://www.kaggle.com/datasets/datasnaek/youtube-new) on Kaggle. Each region has a CSV of daily trending videos (views, likes, dislikes, comments, tags, publish time) and a JSON file mapping category IDs to names. This pipeline processes the CA, GB and US regions. The data is not stored in this repository; download it from Kaggle.

## Setup

1. Create three S3 buckets: raw, cleansed and analytics.
2. Upload the Kaggle files to the raw bucket, with CSVs under a `region=<code>/` prefix.
3. Create a Glue crawler over the raw bucket and run it.
4. Deploy `lambda_function.py` with an S3 trigger on the raw JSON prefix and the AWS SDK for pandas layer. Set these environment variables:

| Variable | Example |
|---|---|
| `s3_cleansed_layer` | `s3://<your-cleansed-bucket>/` |
| `glue_catalog_db_name` | `project_youtube_cleaned` |
| `glue_catalog_table_name` | `cleaned_statistics_reference_data` |

5. Create Glue jobs from the two scripts in `glue/`, updating bucket names, and run `csv_to_parquet.py` then `youtube_analytics_etl.py`.
6. Query `project_youtube_analytics.final_analytics` in Athena and connect QuickSight to it.

## Problems solved along the way

- **Schema mismatch on category ID.** The crawler typed the JSON `id` as a string while the CSV `category_id` was a number, so the join failed. Fixed by casting `id` to a 64-bit integer in the Lambda and applying an explicit mapping in the Glue job.
- **Slow Athena queries on large files.** Fixed by converting to Parquet and partitioning by region, so queries scan only the partitions they need.
- **JSON parsing failures in Athena.** The nested category JSON could not be queried directly, so the Lambda flattens it before writing Parquet.

## Possible next steps

- Pull live data from the YouTube Data API instead of static files.
- Orchestrate the jobs with Step Functions or Airflow.
- Add stronger Glue Data Quality rules than the default column-count check.
