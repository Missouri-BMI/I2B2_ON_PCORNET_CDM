CREATE OR REPLACE FILE FORMAT {stage_schema}.{TSV_FORMAT}
            TYPE=CSV
            FIELD_DELIMITER = '\t'
            ESCAPE=NONE
            NULL_IF = ('NULL')
            COMPRESSION=AUTO
            ESCAPE_UNENCLOSED_FIELD=NONE
            FIELD_OPTIONALLY_ENCLOSED_BY=NONE
            SKIP_HEADER=1;


 CREATE OR REPLACE FILE FORMAT {stage_schema}.{DSV_FORMAT}
            TYPE=CSV
            FIELD_DELIMITER = '\t'
            COMPRESSION = AUTO
            SKIP_HEADER=1
            FIELD_OPTIONALLY_ENCLOSED_BY = '"';

CREATE OR REPLACE STAGE {stage_schema}.{TSV_STAGE} FILE_FORMAT = {stage_schema}.{TSV_FORMAT};

CREATE OR REPLACE STAGE {stage_schema}.{DSV_STAGE} FILE_FORMAT = {stage_schema}.{DSV_FORMAT};

PUT {LOCAL_STAGE}/*.tsv @{stage_schema}.{TSV_STAGE} {PUT_PARAMETERS};

PUT {LOCAL_STAGE}/*.dsv @{stage_schema}.{DSV_STAGE} {PUT_PARAMETERS};