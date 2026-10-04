import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1754973526291 = glueContext.create_dynamic_frame.from_catalog(database="project_youtube_cleaned", table_name="cleaned_statistics_reference_data", transformation_ctx="AWSGlueDataCatalog_node1754973526291")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1754973686977 = glueContext.create_dynamic_frame.from_catalog(database="project_youtube_cleaned", table_name="youtube", transformation_ctx="AWSGlueDataCatalog_node1754973686977")

# Script generated for node Join
Join_node1754973914537 = Join.apply(frame1=AWSGlueDataCatalog_node1754973526291, frame2=AWSGlueDataCatalog_node1754973686977, keys1=["id"], keys2=["category_id"], transformation_ctx="Join_node1754973914537")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=Join_node1754973914537, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1754970032272", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1754974050086 = glueContext.getSink(path="s3://project-on-youtube-analytical-useast1", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=["region", "category_id"], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1754974050086")
AmazonS3_node1754974050086.setCatalogInfo(catalogDatabase="project_youtube_analytics",catalogTableName="final_analytics")
AmazonS3_node1754974050086.setFormat("glueparquet", compression="snappy")
AmazonS3_node1754974050086.writeFrame(Join_node1754973914537)
job.commit()