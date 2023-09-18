import datetime
import sys


from database_driver import SnowflakeDatabaseDriver


class PatCountVisitsGenerator:
    def __init__(self, driver: SnowflakeDatabaseDriver) -> None:
        self.driver = driver
        
    def pat_count_visits(self, tabname: str, tableschema):
        
        v_sqlstr = 'create or replace temp table ontPatVisitDims as ' + '\n' \
        ' select c_fullname' + '\n' \
        ', c_basecode' + '\n' \
        ', c_facttablecolumn' + '\n' \
        ', c_tablename' + '\n' \
        ', c_columnname' + '\n' \
        ', c_operator' + '\n' \
        ', c_dimcode' + '\n' \
        ', null::integer as numpats' + '\n' \
        ' from ' + tabname  + '\n' \
        ' where  m_applied_path = \'@\'' + '\n' \
        ' and lower(c_tablename) in (\'patient_dimension\', \'visit_dimension\') '
        
        self.driver.execute(v_sqlstr)
        
        read_sql = 'select c_fullname, c_facttablecolumn, c_tablename, c_columnname, c_operator, c_dimcode from ontPatVisitDims' 

        curRecord = self.driver.fetchall(read_sql)

        for index, row in curRecord.iterrows():
            ## ACT ONTOLGOY AGE AT VISITS, LENGHT OF STAY calculation
            c_dimcode = row['c_dimcode'.upper()].replace('from PATIENT_DIMENSION', 'from '  + tableschema + '.' +'PATIENT_DIMENSION')
            if 'GETDATE' in c_dimcode:
                continue
            
            v_sqlstr = 'update ontPatVisitDims ' + 'set numpats =  ( ' + '\n' \
            ' select count(distinct(patient_num)) ' + '\n' \
            ' from ' + tableschema + '.' + row['c_tablename'.upper()] + '\n' \
            ' where ' + row['c_columnname'.upper()] + ' '
            
            if row['c_columnname'.upper()].lower() == 'birth_date' and \
            row['c_tablename'.upper()].lower() == 'patient_dimension' and \
            'not recorded' in c_dimcode.lower():
                v_sqlstr += ' is null'
            elif row['c_operator'.upper()].lower() == 'like':
                v_sqlstr += row['c_operator'.upper()] + ' ' + '\'' + c_dimcode.replace('\\', '\\\\').replace('\'', '\'\'') + '%\''
            elif row['c_operator'.upper()].lower() == 'in':
                if c_dimcode[0] == '(':
                    v_sqlstr += row['c_operator'.upper()] + ' ' + c_dimcode
                else:
                    v_sqlstr += row['c_operator'.upper()].upper() + ' ' + '(' + c_dimcode + ')'
            elif row['c_operator'.upper()].lower() == '=':
                v_sqlstr += row['c_operator'.upper()] + ' \'' + c_dimcode.replace('\'', '\'\'') +'\''
        
            else:
                v_sqlstr += row['c_operator'.upper()] + ' ' + c_dimcode
            
            v_sqlstr += ' ) ' + ' \nwhere c_fullname = ' + '\'' + row['c_fullname'.upper()].replace('\\', '\\\\').replace('\'', '\'\'') + '\'' + ' and numpats is null;'
            self.driver.execute(v_sqlstr)

        v_sqlstr = 'update ' + tabname + ' a set c_totalnum=b.numpats ' + '\n' \
        + ' from ontPatVisitDims b ' + '\n' \
        + ' where a.c_fullname=b.c_fullname and b.numpats>0;'
        self.driver.execute(v_sqlstr)
        
        print(f"At {datetime.datetime.now()}: Running: {v_sqlstr}")
        
        v_sqlstr = "select count(*) v_num from ontPatVisitDims where numpats is not null and numpats <> 0;"
        curRecord = self.driver.fetchall(v_sqlstr)
        
        v_num = curRecord['v_num'.upper()].iloc[0]
        
        print(f"At {datetime.datetime.now()}, updating c_totalnum in {tabname} for {v_num} records")
        
        v_sqlstr = 'insert into totalnum(c_fullname, agg_date, agg_count, typeflag_cd)' + '\n' \
        'select c_fullname, current_date, numpats, \'PD\' from ontPatVisitDims;'
        self.driver.execute(v_sqlstr)
        
    
