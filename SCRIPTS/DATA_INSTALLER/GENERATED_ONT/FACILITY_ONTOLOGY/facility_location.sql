
CREATE OR REPLACE TEMPORARY TABLE {metadata_schema}.TEMP_FACILITY_LOCATIONS AS
SELECT DISTINCT FACILITY_LOCATION
FROM {source_schema}.DEID_ENCOUNTER;

DELETE FROM {metadata_schema}.ACT_VISIT_DETAILS_V41
WHERE lower(C_FULLNAME) like '%\\facility location\\%';

INSERT INTO {metadata_schema}.ACT_VISIT_DETAILS_V41
SELECT 
    3,
    CONCAT('\\ACT\\Visit Details\\Facility Location\\', FACILITY_LOCATION, '\\'),
    FACILITY_LOCATION,
    'N',
    'LA',
    NULL,
    FACILITY_LOCATION,
    NULL,
    'encounter_num',
    'visit_dimension',
    'facility_location',
    'N',
    '=',
    FACILITY_LOCATION,
    NULL,
    CONCAT('Visit Details\\Facility Location\\', FACILITY_LOCATION, '\\'),
    '@',
    CURRENT_DATE(),
    CURRENT_DATE(),
    CURRENT_DATE(),
    NULL,
    NULL,
    NULL,
    NULL,
    NULL
FROM {metadata_schema}.TEMP_FACILITY_LOCATIONS;

INSERT INTO {metadata_schema}.ACT_VISIT_DETAILS_V41
select
    2,
    '\\ACT\\Visit Details\\Facility Location\\',
    'Facility Location',
    'N',
    'FA',
    NULL,
    NULL,
    NULL,
    'encounter_num',
    'visit_dimension',
    'facility_location',
    'N',
    'IN',
    (select '(' || LISTAGG(FACILITY_LOCATION, ',') || ')' FROM {metadata_schema}.TEMP_FACILITY_LOCATIONS),
    NULL,
    'Visit Details\\Facility Location\\',
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

DROP TABLE IF EXISTS {metadata_schema}.TEMP_FACILITY_LOCATIONS;