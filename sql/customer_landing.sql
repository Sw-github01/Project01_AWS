import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node customer Trusted
customerTrusted_node1686440757983 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stdi-lake-house-prjct01/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="customerTrusted_node1686440757983",
)

# Script generated for node Step Trainer landing to Trusted
StepTrainerlandingtoTrusted_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stdi-lake-house-prjct01/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="StepTrainerlandingtoTrusted_node1",
)

# Script generated for node Join
Join_node1686440834292 = Join.apply(
    frame1=StepTrainerlandingtoTrusted_node1,
    frame2=customerTrusted_node1686440757983,
    keys1=["serialNumber"],
    keys2=["serialNumber"],
    transformation_ctx="Join_node1686440834292",
)

# Script generated for node Amazon S3
AmazonS3_node1686440898044 = glueContext.write_dynamic_frame.from_options(
    frame=Join_node1686440834292,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stdi-lake-house-prjct01/step_trainer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="AmazonS3_node1686440898044",
)

job.commit()
