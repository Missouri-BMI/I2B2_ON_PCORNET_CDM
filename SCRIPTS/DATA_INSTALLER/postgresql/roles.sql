-- sample for i2b2_mu
GRANT ALL PRIVILEGES ON DATABASE i2b2_mu TO application;

GRANT ALL PRIVILEGES ON SCHEMA i2b2hive, i2b2pm, i2b2workdata TO application;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA i2b2hive, i2b2pm, i2b2workdata TO application;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA i2b2hive, i2b2pm, i2b2workdata TO application;
ALTER DEFAULT PRIVILEGES IN SCHEMA i2b2hive, i2b2pm, i2b2workdata
GRANT ALL PRIVILEGES ON TABLES TO application;