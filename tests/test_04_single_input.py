import pytest
from tests.testprep import TestPrep
from pipelinehelper.code_lib_spark import CodeMapper

spark, df, config, assertions = TestPrep.prepare("test_assertions_4")
df_1_col = df.select("gender") 

def test_example():  
    print("test 1:1 mapping: custom mapping col defined, returns a dataframe with a single column, custom output column")
    
    cm = CodeMapper(config.get_code_mapping("professions"))
    df_column_mapped = cm.apply_mapping(df_1_col, 
                                    map_input=["col_2"], 
                                    map_target="col_2",
                                    df_target='xy')

    df_column_mapped.show()
    assert list(df_column_mapped.collect()) == list(assertions.collect())
