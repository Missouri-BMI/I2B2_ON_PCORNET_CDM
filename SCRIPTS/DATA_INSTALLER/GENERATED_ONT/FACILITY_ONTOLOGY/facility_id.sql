CREATE OR REPLACE TEMPORARY TABLE {metadata_schema}.TEMP_FACILITY_IDS AS
SELECT DISTINCT FACILITYID
FROM {source_schema}.DEID_ENCOUNTER;

DELETE FROM {metadata_schema}.ACT_VISIT_DETAILS_V41
WHERE lower(C_FULLNAME) like '%\\facility\\%';

INSERT INTO {metadata_schema}.ACT_VISIT_DETAILS_V41
SELECT 
    4,
    CONCAT('\\ACT\\Visit Details\\Facility\\', FACILITYID, '\\'),
    FACILITYID,
    'N',
    'LA',
    NULL,
    FACILITYID,
    NULL,
    'encounter_num',
    'visit_dimension',
    'facilityid',
    'T',
    '=',
    FACILITYID,
    NULL,
    CONCAT('\\ACT\\Visit Details\\Facility\\', FACILITYID, '\\'),
    '@',
    CURRENT_DATE(),
    CURRENT_DATE(),
    CURRENT_DATE(),
    NULL,
    NULL,
    NULL,
    NULL,
    NULL
FROM {metadata_schema}.TEMP_FACILITY_IDS;
 
INSERT INTO {metadata_schema}.ACT_VISIT_DETAILS_V41
select 
    2,
    '\\ACT\\Visit Details\\Facility\\',
    'Facility ID',
    'N',
    'CA',
    NULL,
    NULL,
    NULL,
    'concept_cd',
    'concept_dimension',
    'concept_path',
    'T',
    'LIKE',
    '\\ACT\\Visit Details\\Facility\\',
    NULL,
    '\\ACT\\Visit Details\\Facility\\',
    '@',
    CURRENT_DATE(),
    CURRENT_DATE(),
    CURRENT_DATE(),
    NULL,
    NULL,
    NULL,
    NULL,
    NULL
;


DROP TABLE IF EXISTS {metadata_schema}.TEMP_FACILITY_IDS;


insert into {metadata_schema}.ACT_VISIT_DETAILS_V41
values (3,
        '\\ACT\\Visit Details\\Facility\\0-9\\',
        '0-9',
        'N',
        'FA',
        null,
        null,
        null,
        'encounter_num',
        'visit_dimension',
        'facilityid',
        'N',
        'IN',
        '()',
        null,
        '\\ACT\\Visit Details\\Facility\\0-9\\',
        '@',
        current_date,
        current_date,
        current_date,
        null,
        null,
        null,
        null,
        null),
        (3,
        '\\ACT\\Visit Details\\Facility\\A-E\\',
        'A-E',
        'N',
        'FA',
        null,
        null,
        null,
        'encounter_num',
        'visit_dimension',
        'facilityid',
        'N',
        'IN',
        '()',
        null,
        '\\ACT\\Visit Details\\Facility\\A-E\\',
        '@',
        current_date,
        current_date,
        current_date,
        null,
        null,
        null,
        null,
        null),
        (3,
        '\\ACT\\Visit Details\\Facility\\F-J\\',
        'F-J',
        'N',
        'FA',
        null,
        null,
        null,
        'encounter_num',
        'visit_dimension',
        'facilityid',
        'N',
        'IN',
        '()',
        null,
        '\\ACT\\Visit Details\\Facility\\F-J\\',
        '@',
        current_date,
        current_date,
        current_date,
        null,
        null,
        null,
        null,
        null)
        ,(3,
        '\\ACT\\Visit Details\\Facility\\K-O\\',
        'K-O',
        'N',
        'FA',
        null,
        null,
        null,
        'encounter_num',
        'visit_dimension',
        'facilityid',
        'N',
        'IN',
        '()',
        null,
        '\\ACT\\Visit Details\\Facility\\K-O\\',
        '@',
        current_date,
        current_date,
        current_date,
        null,
        null,
        null,
        null,
        null),(3,
        '\\ACT\\Visit Details\\Facility\\P-T\\',
        'P-T',
        'N',
        'FA',
        null,
        null,
        null,
        'encounter_num',
        'visit_dimension',
        'facilityid',
        'N',
        'IN',
        '()',
        null,
        '\\ACT\\Visit Details\\Facility\\P-T\\',
        '@',
        current_date,
        current_date,
        current_date,
        null,
        null,
        null,
        null,
        null),(3,
        '\\ACT\\Visit Details\\Facility\\U-Z\\',
        'U-Z',
        'N',
        'FA',
        null,
        null,
        null,
        'encounter_num',
        'visit_dimension',
        'facilityid',
        'N',
        'IN',
        '()',
        null,
        '\\ACT\\Visit Details\\Facility\\U-Z\\',
        '@',
        current_date,
        current_date,
        current_date,
        null,
        null,
        null,
        null,
        null);


update {metadata_schema}.act_visit_details_v41
set c_fullname = CASE
        WHEN ASCII(left(lower(c_name), 1)) BETWEEN  ASCII('0') and  ASCII('9') THEN
            replace(c_fullname, '\\ACT\\Visit Details\\Facility\\', '\\ACT\\Visit Details\\Facility\\0-9\\') 
        WHEN ASCII(left(lower(c_name), 1)) BETWEEN  ASCII('a') and  ASCII('e') THEN
            replace(c_fullname, '\\ACT\\Visit Details\\Facility\\', '\\ACT\\Visit Details\\Facility\\A-E\\')
        WHEN ASCII(left(lower(c_name), 1)) BETWEEN  ASCII('f') and  ASCII('j') THEN
            replace(c_fullname, '\\ACT\\Visit Details\\Facility\\', '\\ACT\\Visit Details\\Facility\\F-J\\')
        WHEN ASCII(left(lower(c_name), 1)) BETWEEN  ASCII('k') and  ASCII('o') THEN
            replace(c_fullname, '\\ACT\\Visit Details\\Facility\\', '\\ACT\\Visit Details\\Facility\\K-O\\')
        WHEN ASCII(left(lower(c_name), 1)) BETWEEN  ASCII('p') and  ASCII('t') THEN  
            replace(c_fullname, '\\ACT\\Visit Details\\Facility\\', '\\ACT\\Visit Details\\Facility\\P-T\\')
        WHEN ASCII(left(lower(c_name), 1)) BETWEEN  ASCII('u') and  ASCII('z') THEN
            replace(c_fullname, '\\ACT\\Visit Details\\Facility\\', '\\ACT\\Visit Details\\Facility\\U-Z\\')
    END,
    c_tooltip = CASE
        WHEN ASCII(left(lower(c_name), 1)) BETWEEN  ASCII('0') and  ASCII('9') THEN
            replace(c_tooltip, '\\ACT\\Visit Details\\Facility\\', '\\ACT\\Visit Details\\Facility\\0-9\\') 
        WHEN ASCII(left(lower(c_name), 1)) BETWEEN  ASCII('a') and  ASCII('e') THEN
            replace(c_tooltip, '\\ACT\\Visit Details\\Facility\\', '\\ACT\\Visit Details\\Facility\\A-E\\')
        WHEN ASCII(left(lower(c_name), 1)) BETWEEN  ASCII('f') and  ASCII('j') THEN
            replace(c_tooltip, '\\ACT\\Visit Details\\Facility\\', '\\ACT\\Visit Details\\Facility\\F-J\\')
        WHEN ASCII(left(lower(c_name), 1)) BETWEEN  ASCII('k') and  ASCII('o') THEN
            replace(c_tooltip, '\\ACT\\Visit Details\\Facility\\', '\\ACT\\Visit Details\\Facility\\K-O\\')
        WHEN ASCII(left(lower(c_name), 1)) BETWEEN  ASCII('p') and  ASCII('t') THEN
            replace(c_tooltip, '\\ACT\\Visit Details\\Facility\\', '\\ACT\\Visit Details\\Facility\\P-T\\')
        WHEN ASCII(left(lower(c_name), 1)) BETWEEN  ASCII('u') and  ASCII('z') THEN
            replace(c_tooltip, '\\ACT\\Visit Details\\Facility\\', '\\ACT\\Visit Details\\Facility\\U-Z\\')
    END
where c_columnname = 'facilityid' and c_hlevel = 4;

