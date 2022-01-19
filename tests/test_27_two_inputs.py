import pytest
from tests.testprep import TestPrep
from pipelinehelper.code_lib_spark import CodeMapper

spark, df, config, assertions = TestPrep.prepare("test_assertions_3")
df_2_col = df.select(["professioncode", "gender"]) 

def test_example():
    print("Test 2:1 mapping: df with two columns, and mapping target column defined")
    
    cm = CodeMapper(config.get_code_mapping("professions"))
    df_output = cm.apply_mapping(df_2_col, 
                                map_target="col_1")
    df_output.show()
    assert list(df_output.collect()) == list(assertions.collect())
