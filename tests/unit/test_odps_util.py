from dbt.adapters.odps.utils import parse_hints
import  pytest


def test_parse_hints():
    input_string = '''
    set odps.sql.type.system.odps2=True;
    select * from dual;
    '''
    hints, sql = parse_hints(input_string)
    assert hints == {'odps.sql.type.system.odps2': True}
    assert sql.strip() == "select * from dual;"