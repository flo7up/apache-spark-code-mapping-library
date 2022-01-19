import pytest
from tests.testprep import TestPrep
from pipelinehelper.code_lib_spark import CodeMapper

spark, df, config, assertions = TestPrep.prepare()
df_1_col = df.select("gender") 

def test_example():  
    print("Negative test 1:1 mapping negative: df with a single column given, but two mapping inputs defined")
    
    cm = CodeMapper(config.get_code_mapping("professions"))
    with pytest.raises(ValueError) as excinfo:
        cm.apply_mapping(df_1_col, 
                            map_input=["col_1", "col_2"], 
                            map_target="col_0", 
                            df_target='output_test')
    print(excinfo)
    assert "Error 700" in str(excinfo.value)
