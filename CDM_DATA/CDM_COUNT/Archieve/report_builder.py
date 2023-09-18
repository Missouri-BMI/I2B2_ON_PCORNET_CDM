import sys


from database_driver import SnowflakeDatabaseDriver
from random_normal import RandomNormalGenerator

class ReportBuilder:
    
    def __init__(self, driver: SnowflakeDatabaseDriver) -> None:
        self.db_driver = driver
        
        
    def build_total_num_report(self, threshold: int, sigma: float):
        v_sqlstr = 'truncate table totalnum_report;'
        self.db_driver.execute(v_sqlstr)
        v_sqlstr =  'insert into totalnum_report(c_fullname, agg_count, agg_date)' + '\n' \
        'select c_fullname, case sign(agg_count - '+ str(threshold) + '+ 1 ) when 1 then (round(agg_count/5.0,0)*5)+round('+str(RandomNormalGenerator.random_normal(0,sigma,threshold))+') else -1 end agg_count,' + '\n' \
        'to_char(agg_date,\'YYYY-MM-DD\') agg_date from ' + '\n' \
        '(select * from ' + '\n' \
        '(select row_number() over (partition by c_fullname order by agg_date desc) rn,c_fullname, agg_count,agg_date from totalnum where typeflag_cd like \'P%\') x where rn=1) y;'
        self.db_driver.execute(v_sqlstr)
        v_sqlstr = 'update totalnum_report set agg_count=-1 where agg_count<' + str(threshold)
        self.db_driver.execute(v_sqlstr)