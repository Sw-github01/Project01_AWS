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

# Script generated for node accelerometer landing to trusted
accelerometerlandingtotrusted_node1686439053641 = (
    glueContext.create_dynamic_frame.from_options(
        format_options={"multiline": False},
        connection_type="s3",
        format="json",
        connection_options={
            "paths": ["s3://stdi-lake-house-prjct01/accelerometer/landing/"],
            "recurse": True,
        },
        transformation_ctx="accelerometerlandingtotrusted_node1686439053641",
    )
)

# Script generated for node customer trusted
customertrusted_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stdi-lake-house-prjct01/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="customertrusted_node1",
)

# Script generated for node Join
Join_node1686439104914 = Join.apply(
    frame1=customertrusted_node1,
    frame2=accelerometerlandingtotrusted_node1686439053641,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="Join_node1686439104914",
)

# Script generated for node Amazon S3
AmazonS3_node1686439380505 = glueContext.write_dynamic_frame.from_options(
    frame=Join_node1686439104914,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stdi-lake-house-prjct01/accelerometer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="AmazonS3_node1686439380505",
)

job.commit()
