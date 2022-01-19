import pytest
from tests.testprep import TestPrep
from pipelinehelper.code_lib_spark import CodeMapper

spark, df, config, assertions = TestPrep.prepare()

def test_example():
    print("Negative test 3:1 mapping: df with more than three columns, two df columns defined, but three mapping inputs defined")
    
    cm = CodeMapper(config.get_code_mapping("professions"))
    with pytest.raises(ValueError) as excinfo:
         cm.apply_mapping(df, 
                             df_input=['professioncode', 'gender'], 
                             map_input=['col_1'],
                             map_target="col_0",
                             df_target='new_column_name')
    print(excinfo)
    assert "Error 303" in str(excinfo.value)
