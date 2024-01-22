
-- ADI_NATRANK, ADI_STATERNK are the columns we want to have in i2b2 for querying
-- Create SDOH_FACT view that can be used later for ACS data as well
-- Below tables will be used. I will share them to i2b2 SF

--ADI data table
USE ROLE ANALYTICS;
USE WAREHOUSE ANALYTICS_WH;
select GISJOIN, FIPS, ADI_NATRANK, ADI_STATERNK from ACS.ADI.ADI_2021;

--GEOCODED ADDRESS Tables
select PATID, ADDRESS, MATCHED_STREET, MATCHED_ZIP, MATCHED_CITY, MATCHED_STATE, LAT, LON, SCORE, PRECISION, STREET1, STREET2, GEOCODE_RESULT
from GEO_CODING.DEGAUSS_TRANSFORMATION.GEOCODED_ADDRESS;

select PATID, ADDRESS, FIPS_BLOCK_GROUP_ID_2020, FIPS_TRACT_ID_2020
from PCORNET_CDM.CDM_2023_NOV.PRIVATE_GEOCODED_ADDRESS;

--Joining the tables to create SDOH_FACT view in i2b2 DB
with ADI as (
    select g.patid, a.* from ACS.ADI.ADI_2021 a
    left join PCORNET_CDM.CDM_2023_NOV.PRIVATE_GEOCODED_ADDRESS g
    where (g.fips_block_group_id_2020) = to_varchar(a.fips)
) 
Select distinct ADI_STATERNK from ADI
order by ADI_STATERNK;
