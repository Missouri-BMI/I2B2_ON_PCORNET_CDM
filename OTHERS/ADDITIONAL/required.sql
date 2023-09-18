

/* hide folders

ACT Diagnoses ICD10-ICD9
ACT Procedures HCPCS
*/

update i2b2metadata.table_access
set c_visualattributes = 'FH'
where c_table_name = 'NCATS_ICD10_ICD9_DX_V1' or c_table_name = 'ACT_HCPCS_PX_2018AA'

/* shared

*/

update i2b2workdata.workplace_access
set c_group_id = 'ACT'
where c_user_id = 'shared';


/* rename project */
update i2b2pm.pm_project_data
set project_name = 'NextGen Data Lake De-Identified'
where project_id = 'ACT';

