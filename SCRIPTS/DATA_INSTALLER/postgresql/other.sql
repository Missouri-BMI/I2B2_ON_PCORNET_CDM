-- version 1.8.2 comatibility script
ALTER TABLE i2b2_prod.i2b2data.QT_BREAKDOWN_PATH
ADD COLUMN group_id VARCHAR(50);


ALTER TABLE i2b2_sandbox_gpc.i2b2data.QT_BREAKDOWN_PATH
ADD COLUMN group_id VARCHAR(50);


update i2b2hive.work_db_lookup
set c_db_servertype = 'POSTGRESQL';