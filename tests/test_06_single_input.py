import pytest
from tests.testprep import TestPrep
from pipelinehelper.code_lib_spark import CodeMapper

spark, df, config, assertions = TestPrep.prepare("test_assertions_5")
df_1_col = df.select("professioncode") 

def test_example():  
    print("Test 1:1 mapping: col defined, returns a dataframe with a single defined input")
    
    cm = CodeMapper(config.get_code_mapping("professions"))
    df_column_mapped = cm.apply_mapping(df_1_col, 
                                    map_input=["col_1"], 
                                    map_target="col_3", 
                                    df_target='output_test')
    df_column_mapped.show()
    assert list(df_column_mapped.collect()) == list(assertions.collect())
