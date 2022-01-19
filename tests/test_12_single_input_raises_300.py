import pytest
from tests.testprep import TestPrep
from pipelinehelper.code_lib_spark import CodeMapper

spark, df, config, assertions = TestPrep.prepare()
df_1_col = df.select("gender") 

def test_example():  
    print("Negative test 1:1 mapping: map input outside list of allowed values")
    
    cm = CodeMapper(config.get_code_mapping("professions"))
    with pytest.raises(ValueError) as excinfo:
        cm.apply_mapping(df_1_col, 
                                    map_input=["xy"], 
                                    map_target="col_0")
    print(excinfo)
    assert "Error 300" in str(excinfo.value)