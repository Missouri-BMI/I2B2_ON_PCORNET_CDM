import datetime
import sys

from database_driver import SnowflakeDatabaseDriver

class PatCountDimensionsGenerator:
    
    def __init__(self, driver: SnowflakeDatabaseDriver) -> None:
        self.driver = driver
        
    def pat_count_dimensions(self, metadataTable: str, schemaName: str, observationTable: str, facttablecolumn: str, tablename: str, columnname: str ):
        v_startime = datetime.datetime.now()
        print(f"At {v_startime}, running PAT_COUNT_DIMENSIONS(\'{metadataTable}\')")

        v_sqlstr = 'create or replace temp table dimCountOnt AS ' + '\n' \
        ' select c_fullname, c_basecode, c_hlevel ' + '\n' \
        ' from ' + metadataTable  + '\n' \
        ' where lower(c_facttablecolumn) like \'%' + facttablecolumn + '\' ' + '\n' \
        ' and lower(c_tablename) = \''+ tablename + '\' ' + '\n' \
        ' and lower(c_columnname) = \''+ columnname + '\' ' + '\n' \
        ' and lower(c_synonym_cd) = \'n\' ' + '\n' \
        ' and lower(c_columndatatype) = \'t\' ' + '\n' \
        ' and lower(c_operator) = \'like\' ' + '\n' \
        ' and m_applied_path = \'@\' ' + '\n' \
        ' and coalesce(c_fullname, \'\') <> \'\' ' + '\n' \
        ' and (c_visualattributes not like \'L%\' or  c_basecode in (select distinct concept_cd from ' + schemaName.lower() + '.' + observationTable + ')) ;'
        print(f"SQL: {v_sqlstr}")
        self.driver.execute(v_sqlstr)
        

        v_sqlstr = "create or replace temp table dimOntWithFolders AS" + '\n' \
        "select distinct p1.c_fullname, p1.c_basecode" + '\n' \
        "from dimCountOnt p1" + '\n' \
        "where 1=0;"
        self.driver.execute(v_sqlstr)
            
        read_sql = 'select c_fullname,c_table_name from table_access;' 

        curRecord = self.driver.fetchall(read_sql)
        for index, row in curRecord.iterrows():
            if row['c_table_name'.upper()] == metadataTable:
                v_sqlstr = ' insert into dimOntWithFolders ' + '\n' \
                'with recursive concepts as (' + '\n' \
                ' select c_fullname, c_hlevel, c_basecode ' + '\n' \
                '  from dimCountOnt ' + '\n' \
                '  where c_fullname like \'' + row['c_fullname'.upper()].replace('\\', '\\\\').replace('\'', '\'\'') + '%\'' + '\n' \
                ' union all ' + '\n' \
                ' select cast( ' + '\n' \
                'left(c_fullname, length(c_fullname)-position(\'\\\\\' in right(reverse(c_fullname), length(c_fullname)-1))) ' + '\n' \
                'as varchar(700) ' + '\n' \
                ') c_fullname, ' + '\n' \
                ' c_hlevel-1 c_hlevel, c_basecode ' + '\n' \
                ' from concepts ' + '\n' \
                ' where concepts.c_hlevel>0 ' + '\n' \
                ' ) ' + '\n' \
                ' select distinct c_fullname, c_basecode ' + '\n' \
                '  from concepts ' + '\n' \
                '  where c_fullname like \'' + row['c_fullname'.upper()].replace('\\', '\\\\').replace('\'', '\'\'') + '%\'' + '\n' \
                '  order by c_fullname, c_basecode; '
                print(f"SQL_dimOntWithFolders: {v_sqlstr}")
                self.driver.execute(v_sqlstr)
            
                v_duration = datetime.datetime.now() - v_startime
                print(f"(BENCH) {metadataTable},collected_concepts,{v_duration}")
                v_startime = datetime.datetime.now()
        
        v_sqlstr = 'create or replace temp table Path2Num as' + '\n' \
        'select c_fullname, row_number() over (order by c_fullname) path_num' + '\n' \
        'from ( ' + '\n' \
        '        select distinct c_fullname c_fullname' + '\n' \
        '        from dimOntWithFolders' + '\n' \
        '        where c_fullname is not null and c_fullname<>\'\'' + '\n' \
        '    ) t;'
        self.driver.execute(v_sqlstr)
        
        v_sqlstr = 'create or replace temp table ConceptPath as' + '\n' \
        'select path_num,c_basecode from Path2Num n inner join dimontwithfolders o on o.c_fullname=n.c_fullname' +  '\n' \
        'where o.c_fullname is not null and c_basecode is not null;'
        self.driver.execute(v_sqlstr)
                    
                    
        v_sqlstr = 'create or replace temp table PathCounts as select p1.path_num, count(distinct patient_num) as num_patients  from ConceptPath p1  left join ' + schemaName.lower() + '.'+ observationTable + '  o      on p1.c_basecode = o.concept_cd     and coalesce(p1.c_basecode, \'\') <> \'\'  group by p1.path_num;'
        self.driver.execute(v_sqlstr)
                    
        v_sqlstr = 'create or replace temp table finalCountsbyConcept as' + '\n' \
        'select p.c_fullname, c.num_patients num_patients ' + '\n' \
        '    from PathCounts c' + '\n' \
        '      inner join Path2Num p' + '\n' \
        '       on p.path_num=c.path_num' + '\n' \
        '    order by p.c_fullname;'
        self.driver.execute(v_sqlstr)
                    
        v_duration = datetime.datetime.now() - v_startime
        print(f"(BENCH) {metadataTable},counted_concepts,{v_duration}")
        v_startime = datetime.datetime.now()
        
        v_sqlstr = ' update ' + metadataTable + ' a set c_totalnum=b.num_patients ' + '\n' \
        ' from finalCountsbyConcept b ' + '\n' \
        ' where a.c_fullname=b.c_fullname ' + '\n' \
        ' and lower(a.c_facttablecolumn) like \'%' + facttablecolumn + '\' ' + '\n' \
        ' and lower(a.c_tablename) = \'' + tablename + '\' ' + '\n' \
        ' and lower(a.c_columnname) = \'' + columnname + '\';'
        self.driver.execute(v_sqlstr)
                    
        v_sqlstr = 'select count(*) v_num from finalCountsByConcept where num_patients is not null and num_patients <> 0;'
        curRecord = self.driver.fetchall(v_sqlstr)
        v_num = curRecord['v_num'.upper()].iloc[0]
        print(f"At {datetime.datetime.now()}, updating c_totalnum in {metadataTable} {v_num}")
                    
                    
        v_sqlstr = 'insert into totalnum(c_fullname, agg_date, agg_count, typeflag_cd)' + '\n' \
        'select c_fullname, current_date, num_patients, \'PF\' from finalCountsByConcept where num_patients>0;'
        self.driver.execute(v_sqlstr)
        
    