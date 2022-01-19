import pytest
from tests.testprep import TestPrep
from pipelinehelper.code_lib_spark import CodeMapper

spark, df, config, assertions = TestPrep.prepare("test_assertions_10")
df_3_col = df.select(["professioncode", "gender", "profession_type"]) 

def test_example():
    print("Test 2:1 mapping: df with three columns, target column defined")
    
    cm = CodeMapper(config.get_code_mapping("professions"))
    df_output = cm.apply_mapping(df_3_col, 
                            df_input=['professioncode', 'gender'], 
                            map_input=["col_1", "col_2"], 
                            map_target="col_0")
    df_output.show()
    assert list(df_output.collect()) == list(assertions.collect())
