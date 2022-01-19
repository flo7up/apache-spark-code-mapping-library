import pytest
from tests.testprep import TestPrep
from pipelinehelper.code_lib_spark import CodeMapper

spark, df, config, assertions = TestPrep.prepare("test_assertions_10")

def test_example():
    print("Test 2:1 mapping: df with more than two columns, df columns defined, target column defined")
    
    cm = CodeMapper(config.get_code_mapping("professions"))
    df_output = cm.apply_mapping(df, 
                             df_input=['gender', 'professioncode'], 
                             map_input=["col_2", "col_1"], 
                             map_target="col_0")
    df_output.show()
    #df_output.write.format("csv").option("header", True).save("code mapping library/tests/files")
    assert list(df_output.collect()) == list(assertions.collect())
