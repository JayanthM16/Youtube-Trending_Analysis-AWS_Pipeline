import os
import json
import boto3
import pandas as pd
import urllib.parse
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO

# Env vars (Lambda will provide these)
os_input_s3_cleansed_layer = os.environ['s3_cleansed_layer']  # Example: s3://project-on-youtube-cleaned-useast1/
os_input_glue_catalog_db_name = os.environ['glue_catalog_db_name']
os_input_glue_catalog_table_name = os.environ['glue_catalog_table_name']

s3_client = boto3.client("s3")
glue_client = boto3.client("glue")

CLEANSSED_SUBFOLDER = "cleansed_statistics_reference_data/"

def ensure_database_exists(db_name):
    """Check if Glue database exists, create if missing."""
    try:
        glue_client.get_database(Name=db_name)
        print(f"Glue database '{db_name}' already exists.")
    except glue_client.exceptions.EntityNotFoundException:
        glue_client.create_database(
            DatabaseInput={
                "Name": db_name,
                "Description": "Database for cleansed YouTube data"
            }
        )
        print(f"Glue database '{db_name}' created.")

def create_or_update_table(db_name, table_name, s3_location, df):
    """Create or update Glue table based on DataFrame schema."""
    columns = []
    for col, dtype in df.dtypes.items():
        if "int" in str(dtype).lower():
            col_type = "bigint"
        elif "float" in str(dtype).lower():
            col_type = "double"
        elif "bool" in str(dtype).lower():
            col_type = "boolean"
        else:
            col_type = "string"
        columns.append({"Name": col, "Type": col_type})

    table_input = {
        "Name": table_name,
        "StorageDescriptor": {
            "Columns": columns,
            "Location": s3_location,
            "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
            "SerdeInfo": {
                "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
            }
        },
        "TableType": "EXTERNAL_TABLE"
    }

    try:
        glue_client.get_table(DatabaseName=db_name, Name=table_name)
        glue_client.update_table(DatabaseName=db_name, TableInput=table_input)
        print(f"Glue table '{table_name}' updated in database '{db_name}'.")
    except glue_client.exceptions.EntityNotFoundException:
        glue_client.create_table(DatabaseName=db_name, TableInput=table_input)
        print(f"Glue table '{table_name}' created in database '{db_name}'.")

def lambda_handler(event, context):
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')

    try:
        # Ensure Glue database exists
        ensure_database_exists(os_input_glue_catalog_db_name)

        # Read JSON from S3
        obj = s3_client.get_object(Bucket=bucket, Key=key)
        json_data = json.loads(obj['Body'].read().decode('utf-8'))

        # Flatten JSON and keep required fields
        df_step_1 = pd.json_normalize(json_data['items'], sep=".")
        fields_to_keep = [
            'kind', 'etag', 'id',
            'snippet.channelId', 'snippet.title', 'snippet.assignable'
        ]
        df_step_1 = df_step_1[fields_to_keep]

        # Convert 'id' to bigint-compatible integer
        df_step_1['id'] = pd.to_numeric(df_step_1['id'], errors='coerce').astype('Int64')

        # Rename for Athena compatibility
        df_step_1.columns = [c.replace(".", "_") for c in df_step_1.columns]

        # Save DataFrame to Parquet in memory
        table = pa.Table.from_pandas(df_step_1)
        out_buffer = BytesIO()
        pq.write_table(table, out_buffer)
        out_buffer.seek(0)

        # Upload Parquet to cleansed S3 path
        cleansed_bucket = os_input_s3_cleansed_layer.replace("s3://", "").split("/")[0]
        output_key = CLEANSSED_SUBFOLDER + os.path.basename(key).replace(".json", ".parquet")
        s3_client.put_object(
            Bucket=cleansed_bucket,
            Key=output_key,
            Body=out_buffer.getvalue()
        )

        # Update Glue table
        s3_location = f"s3://{cleansed_bucket}/{CLEANSSED_SUBFOLDER}"
        create_or_update_table(
            os_input_glue_catalog_db_name,
            os_input_glue_catalog_table_name,
            s3_location,
            df_step_1
        )

        return {
            "statusCode": 200,
            "body": f"Data written to {s3_location} and Glue table updated with all required fields (id as bigint)."
        }

    except Exception as e:
        print(f"Error processing file {key} from bucket {bucket}: {str(e)}")
        return {"statusCode": 500, "body": str(e)}
