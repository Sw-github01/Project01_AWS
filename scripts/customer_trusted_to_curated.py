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

# Script generated for node accelerometer_landing_zone
accelerometer_landing_zone_node1686525806271 = (
    glueContext.create_dynamic_frame.from_options(
        format_options={"multiline": False},
        connection_type="s3",
        format="json",
        connection_options={
            "paths": ["s3://stdi-lake-house-prjct01/accelerometer/trusted/"],
            "recurse": True,
        },
        transformation_ctx="accelerometer_landing_zone_node1686525806271",
    )
)

# Script generated for node customer_trusted_zone
customer_trusted_zone_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stdi-lake-house-prjct01/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="customer_trusted_zone_node1",
)

# Script generated for node Join
Join_node1686525858777 = Join.apply(
    frame1=customer_trusted_zone_node1,
    frame2=accelerometer_landing_zone_node1686525806271,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="Join_node1686525858777",
)

# Script generated for node Drop Fields
DropFields_node1686533416631 = DropFields.apply(
    frame=Join_node1686525858777,
    paths=["z", "timeStamp", "user", "y", "x"],
    transformation_ctx="DropFields_node1686533416631",
)

# Script generated for node customer_curated
customer_curated_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1686533416631,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stdi-lake-house-prjct01/customer/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="customer_curated_node3",
)

job.commit()
