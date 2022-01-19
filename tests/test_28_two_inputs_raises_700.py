import pytest
from tests.testprep import TestPrep
from pipelinehelper.code_lib_spark import CodeMapper

spark, df, config, assertions = TestPrep.prepare()
df_2_col = df.select(["professioncode", "gender"]) 

def test_example():
    print("Negative test 2:1 mapping: df with two columns but three df inputs defined")
    
    cm = CodeMapper(config.get_code_mapping("professions"))
    with pytest.raises(ValueError) as excinfo:
         cm.apply_mapping(df_2_col, 
                                df_input=["gender", "professioncode", "comment"],
                                map_target="col_1")
    print(excinfo)
    assert "Error 700" in str(excinfo.value)

