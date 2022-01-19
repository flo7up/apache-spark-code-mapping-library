from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pipelinehelper.code_lib_spark import ConfigReader

class TestPrep:

    @staticmethod
    def prepare(test_assertions_file=""):
        spark = SparkSession(SparkContext.getOrCreate())
        df = spark.read.option("inferSchema",True) \
        .option("delimiter",";") \
        .option("header", True) \
        .csv("tests/files/claims_test.csv")

        if test_assertions_file != "":
            assertions = spark.read.option("inferSchema",True) \
            .option("delimiter",";") \
            .option("header", True) \
            .csv("tests/files/" + test_assertions_file + ".csv")
        else:
            assertions = None

        code_mapping_path = "tests/files/code_mapping.csv"
        config = ConfigReader(code_mapping_path)
        return spark, df, config, assertions