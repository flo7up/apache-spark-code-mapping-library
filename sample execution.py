from pipelinehelper.code_lib_spark import CodeMapper
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
    
spark, df, config, assertions = TestPrep.prepare("test_assertions_6")

def test_example():
    print("Test 2:1 mapping: df with more than two columns, df columns defined, target column defined")
    
    # cm = CodeMapper(config.get_code_mapping("professions"))
    # df_output = cm.apply_mapping(df, 
    #                          df_input=['gender', 'professioncode'], 
    #                          map_input=["col_2", "col_1"], 
    #                          map_target="col_0")
    # df_output.show()
    # df_output.toPandas().to_csv('mycsv.csv')
   
    df_3_col = df.select(["professioncode", "gender", "profession_type"]) 
    config.get_code_mapping("professions").show()
    
    

    df_1_col = df.select('professioncode')
    cm = CodeMapper(config.get_code_mapping("professions"))
    df_output = cm.apply_mapping(df,    
            df_input=['professioncode'], 
            map_input=['col_1'])
    df_output.show()        
    
test_example()