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

# Script generated for node step trainer trusted
steptrainertrusted_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stdi-lake-house-prjct01/step_trainer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="steptrainertrusted_node1",
)

# Script generated for node accelerometer trusted
accelerometertrusted_node1686527826756 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stdi-lake-house-prjct01/accelerometer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="accelerometertrusted_node1686527826756",
)

# Script generated for node customer with step trainer
customerwithsteptrainer_node1686527763799 = Join.apply(
    frame1=steptrainertrusted_node1,
    frame2=accelerometertrusted_node1686527826756,
    keys1=["sensorReadingTime"],
    keys2=["timeStamp"],
    transformation_ctx="customerwithsteptrainer_node1686527763799",
)

# Script generated for node Drop Fields
DropFields_node1686534626947 = DropFields.apply(
    frame=customerwithsteptrainer_node1686527763799,
    paths=["user", "email", "customerName"],
    transformation_ctx="DropFields_node1686534626947",
)

# Script generated for node customer with step trainer accelerometer
customerwithsteptraineraccelerometer_node1686527956884 = (
    glueContext.write_dynamic_frame.from_options(
        frame=DropFields_node1686534626947,
        connection_type="s3",
        format="json",
        connection_options={
            "path": "s3://stdi-lake-house-prjct01/machine_learning_curated/",
            "partitionKeys": [],
        },
        transformation_ctx="customerwithsteptraineraccelerometer_node1686527956884",
    )
)

job.commit()
