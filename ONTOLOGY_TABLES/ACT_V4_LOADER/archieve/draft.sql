update I2B2METADATA.ACT_COVID_V4
set c_basecode = 
	CASE 
		WHEN c_dimcode = 'O' THEN concat('VISIT|TYPE:', 'AV')
		WHEN c_dimcode = 'I' THEN concat('VISIT|TYPE:', 'IP')
		WHEN c_dimcode = 'E' THEN concat('VISIT|TYPE:', 'ED')
		WHEN c_dimcode = 'EI' THEN concat('VISIT|TYPE:', 'EI')
	end
    , c_facttablecolumn = 'covid_fact.concept_cd'
    , c_tablename = 'concept_dimension'
    , c_columnname = 'concept_path'
    , c_columndatatype = 'T'
    , c_operator = 'LIKE'
    , c_dimcode = c_fullname
where c_columnname = 'inout_cd';


update I2B2METADATA.ACT_COVID_V4
set c_facttablecolumn = 'patient_num'
where c_columnname = 'patient_num';

insert into  I2B2_PROD.I2B2DATA.concept_dimension ( concept_path, concept_cd, name_char, concept_blob, update_date, download_date, import_date, sourcesystem_cd, upload_id)
        select
            c_dimcode
            , c_basecode
            , c_name
            , null
            , current_timestamp
            , current_timestamp
            , current_timestamp
            , 'ACT'
            , null
        from I2B2_PROD.I2B2METADATA.ACT_COVID_V4
        WHERE C_BASECODE LIKE 'VISIT|TYPE:%';