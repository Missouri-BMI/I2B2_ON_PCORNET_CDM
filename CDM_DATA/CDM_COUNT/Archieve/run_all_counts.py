import datetime
import sys
from collections import defaultdict

from database_driver import SnowflakeDatabaseDriver
from db_credential import SnowflakeCredential
from db_credential import DatabaseCredentialManager

from report_builder import ReportBuilder
from pat_count_dimensions import PatCountDimensionsGenerator
from pat_count_visits import PatCountVisitsGenerator

class CountGenerator:
    
    def __init__(self, driver: SnowflakeDatabaseDriver) -> None:
        self.db_driver = driver
        self.visit_count = PatCountVisitsGenerator(driver)
        self.dim_count = PatCountDimensionsGenerator(driver)
        self.report_builder = ReportBuilder(driver)

    def run_total_num(self, observationTable: str, schemaName: str, tableName: str = '@'):
        v_startime = datetime.datetime.now()
        print(f"At {v_startime}, running RunTotalnum()")
        read_sql =  'select distinct c_table_name as sqltext' + '\n' \
        'from TABLE_ACCESS' + '\n' \
        'where c_visualattributes like \'%A%\';' 

        table_access_data = self.db_driver.fetchall(read_sql)

        for index, row in table_access_data.iterrows():
            if tableName == '@' or tableName == row['SQLTEXT']:
                self.visit_count.pat_count_visits(row['SQLTEXT'], schemaName)
                v_duration = datetime.datetime.now() - v_startime
                print( f"info (BENCH) {row['SQLTEXT']},PAT_COUNT_VISITS,{v_duration}")
                v_startime = datetime.datetime.now()
                
                self.dim_count.pat_count_dimensions(row['SQLTEXT'], schemaName, observationTable, 'concept_cd', 'concept_dimension', 'concept_path')
                v_duration = datetime.datetime.now() - v_startime
                print( f"info (BENCH) {row['SQLTEXT']},PAT_COUNT_concept_dimension,{v_duration}")
                v_startime = datetime.datetime.now()
                
                self.dim_count.pat_count_dimensions(row['SQLTEXT'], schemaName, observationTable, 'provider_id', 'provider_dimension', 'provider_path')
                v_duration = datetime.datetime.now() - v_startime
                print( f"info (BENCH) {row['SQLTEXT']},PAT_COUNT_provider_dimension,{v_duration}")
                v_startime = datetime.datetime.now()
                
                self.dim_count.pat_count_dimensions(row['SQLTEXT'], schemaName, observationTable, 'modifier_cd', 'modifier_dimension', 'modifier_path')
                v_duration = datetime.datetime.now() - v_startime
                print( f"info (BENCH) {row['SQLTEXT']},PAT_COUNT_modifier_dimension,{v_duration}")
                v_startime = datetime.datetime.now()
                
                sql_text = 'update table_access' + '\n' \
                'set c_totalnum = (' + '\n' \
                'select min(c_totalnum) from ' + row['SQLTEXT'] + \
                ' x where x.c_fullname=table_access.c_fullname)' + '\n' \
                'where c_table_name = ' + '\'' + row['SQLTEXT'] + '\''
                self.db_driver.execute(sql_text)
                
                sql_text = 'update ' +  row['SQLTEXT'] + '\n' \
                'set c_totalnum=null where c_totalnum=0 and c_visualattributes like' + '\'C%\''
                self.db_driver.execute(sql_text)
                
                
        sql_text = 'update table_access set c_totalnum=null where c_totalnum=0;'
        self.db_driver.execute(sql_text)
        
        sql_text = 'SELECT count(*) denom from totalnum where c_fullname=\'\\\\denominator\\\\facts\\\\\' and agg_date=CURRENT_DATE;'
        curRecord = self.db_driver.fetchall(sql_text)
        denom = curRecord['denom'.upper()].iloc[0]
        
        if denom == 0:
            sql_text = 'insert into totalnum(c_fullname,agg_date,agg_count,typeflag_cd)' + '\n' \
                'select  \'\\\\denominator\\\\facts\\\\\',CURRENT_DATE,count(distinct patient_num),\'PX\' from ' + schemaName + '.' + observationTable
            self.db_driver.execute(sql_text)
            
        self.report_builder.build_total_num_report(10, 6.5)

        
    def generateCountOnOneFactView(self):
        self.clearCache()
        schema = 'I2B2DATA'
        fact_view = 'observation_fact_view'
        
        # Create one fact view
        self.db_driver.execute(
            f"""
                create or replace view {schema}.{fact_view} as
                select * from {schema}.DEMOGRAPHIC_FACT
                union all
                select * from {schema}.VISIT_FACT
                union all
                select * from {schema}.DIAGNOSIS_FACT
                union all
                select * from {schema}.PROCEDURE_FACT
                union all
                select * from {schema}.LAB_FACT
                union all
                select * from {schema}.PRESCRIBING_FACT
                union all
                select * from {schema}.COVID_FACT
                union all
                select * from {schema}.VITAL_FACT
            """
        )
        # execute generate count on fact view
        self.run_total_num(fact_view, schema)
        
        # drop the table
        self.db_driver.execute(
            f"""drop view if exists {schema}.{fact_view}"""
        )
        
       
        print('Count refresh completed...')
    
    def clearCache(self):
        print('Clearing cache counts....')
        sql = 'delete from totalnum'
        self.db_driver.execute(sql)
        sql = 'delete from totalnum_report'
        self.db_driver.execute(sql) 
     
    def generateCountOnIndividualFactView(self):
        
        self.clearCache()
        schema = 'I2B2DATA'
        df = self.db_driver.fetchall('select c_table_name, c_facttablecolumn from table_access where c_visualattributes like \'%A%\';')
        
        
        for i, row in df.iterrows():
            # consider '<fact_table>.concept_cd', ignore 'concept_cd'
            splits = row['C_FACTTABLECOLUMN'].split('.')
            if len(splits) < 2:
                continue
            fact_table = splits[0]
            ont_table = row['C_TABLE_NAME']
            print(f'Generating count for {ont_table} from {fact_table}...')
            self.run_total_num(fact_table, schema, ont_table)
            
        
def main():
    print('Connecting to DB...')
    credential = DatabaseCredentialManager.read_credential_from_env()
    driver = SnowflakeDatabaseDriver(credential)
    driver.connect()
    print('Connected...')

    print('Count refresh started running...')
    count_generator = CountGenerator(driver)
    count_generator.generateCountOnIndividualFactView()
    # close connection
    driver.close()
    
if __name__ == "__main__":
    main()
    