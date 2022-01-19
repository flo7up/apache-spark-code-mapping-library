import pytest
from tests.testprep import TestPrep
from pipelinehelper.code_lib_spark import CodeMapper

spark, df, config, assertions = TestPrep.prepare("test_assertions_2")
df_1_col = df.select("gender")

def test_example():  
    print("Test 1:1 mapping: single col defined, returns a dataframe with a single column")
    
    cm = CodeMapper(config.get_code_mapping("professions"))
    df_column_mapped = cm.apply_mapping(df_1_col, 
                                        map_input=["col_2"])
    df_column_mapped.show()
    assert list(df_column_mapped.collect()) == list(assertions.collect())
