-- Check row count in QT_PATIENT_ENC_COLLECTION:
-- SELECT max(PATIENT_ENC_COLL_ID) FROM QT_PATIENT_ENC_COLLECTION;
-- If it has over 2 billion rows, generating new sets will fail.
-- → Solution: Delete old or unneeded encounter sets.

-- Some sites have found that truncating pm_user_login 
-- and pm_user_session dramatically speeds up queries. 
-- These can be rotated (backed up and truncated) on a regular basis 
-- to keep queries fast. pm_user_login is an audit log of login events, 
-- and pm_user_session tracks active sessions. (Note that completely 
-- truncating pm_user_session will log out all users.)

use database i2b2_dev;

use schema i2b2data;
truncate QT_PATIENT_ENC_COLLECTION;
truncate QT_PATIENT_SET_COLLECTION;

use schema i2b2pm;
truncate PM_USER_LOGIN;
truncate PM_USER_SESSION;


use database i2b2_shrine_mu_dev;

use schema i2b2data;
truncate QT_PATIENT_ENC_COLLECTION;
truncate QT_PATIENT_SET_COLLECTION;

use schema i2b2pm;
truncate PM_USER_LOGIN;
truncate PM_USER_SESSION;


use database i2b2_shrine_washu_dev;

use schema i2b2data;
truncate QT_PATIENT_ENC_COLLECTION;
truncate QT_PATIENT_SET_COLLECTION;

use schema i2b2pm;
truncate PM_USER_LOGIN;
truncate PM_USER_SESSION;

use database i2b2_prod;

use schema i2b2data;
truncate QT_PATIENT_ENC_COLLECTION;
truncate QT_PATIENT_SET_COLLECTION;

use schema i2b2pm;
truncate PM_USER_LOGIN;
truncate PM_USER_SESSION;


use database i2b2_sandbox_gpc;

use schema i2b2data;
truncate QT_PATIENT_ENC_COLLECTION;
truncate QT_PATIENT_SET_COLLECTION;


use database i2b2_shrine_mu_prod;

use schema i2b2data;
truncate QT_PATIENT_ENC_COLLECTION;
truncate QT_PATIENT_SET_COLLECTION;

use schema i2b2pm;
truncate PM_USER_LOGIN;
truncate PM_USER_SESSION;


use database i2b2_shrine_washu_prod;

use schema i2b2data;
truncate QT_PATIENT_ENC_COLLECTION;
truncate QT_PATIENT_SET_COLLECTION;

use schema i2b2pm;
truncate PM_USER_LOGIN;
truncate PM_USER_SESSION;

