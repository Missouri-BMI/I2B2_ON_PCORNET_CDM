
-- delete from act_visit_details_v4
-- where c_columnname in ('facilityid', 'facility_location');


insert into I2B2METADATA.ACT_VISIT_DETAILS_V4
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


update act_visit_details_v4
set c_fullname = CASE
        WHEN ASCII(left(lower(c_name), 1)) BETWEEN  ASCII('0') and  ASCII('9') 
            THEN insert(c_fullname,length('\\ACT\\Visit Details\\Facility\\') + 2, 0, '0-9\\')

        WHEN ASCII(left(lower(c_name), 1)) BETWEEN  ASCII('a') and  ASCII('e')
            THEN insert(c_fullname,length('\\ACT\\Visit Details\\Facility\\') + 2, 0, 'A-E\\')
        WHEN ASCII(left(lower(c_name), 1)) BETWEEN  ASCII('f') and  ASCII('j')
            THEN insert(c_fullname,length('\\ACT\\Visit Details\\Facility\\') + 2, 0, 'F-J\\')
        WHEN ASCII(left(lower(c_name), 1)) BETWEEN  ASCII('k') and  ASCII('o')
            THEN insert(c_fullname,length('\\ACT\\Visit Details\\Facility\\') + 2, 0, 'K-O\\')
            WHEN ASCII(left(lower(c_name), 1)) BETWEEN  ASCII('p') and  ASCII('t')
            THEN insert(c_fullname,length('\\ACT\\Visit Details\\Facility\\') + 2, 0, 'P-T\\')
            WHEN ASCII(left(lower(c_name), 1)) BETWEEN  ASCII('u') and  ASCII('z')
            THEN insert(c_fullname,length('\\ACT\\Visit Details\\Facility\\') + 2, 0, 'U-Z\\')
    END,
    c_tooltip = CASE
        WHEN ASCII(left(lower(c_name), 1)) BETWEEN  ASCII('0') and  ASCII('9') 
            THEN insert(c_fullname,length('\\ACT\\Visit Details\\Facility\\') + 2, 0, '0-9\\')

        WHEN ASCII(left(lower(c_name), 1)) BETWEEN  ASCII('a') and  ASCII('e')
            THEN insert(c_fullname,length('\\ACT\\Visit Details\\Facility\\') + 2, 0, 'A-E\\')
        WHEN ASCII(left(lower(c_name), 1)) BETWEEN  ASCII('f') and  ASCII('j')
            THEN insert(c_fullname,length('\\ACT\\Visit Details\\Facility\\') + 2, 0, 'F-J\\')
        WHEN ASCII(left(lower(c_name), 1)) BETWEEN  ASCII('k') and  ASCII('o')
            THEN insert(c_fullname,length('\\ACT\\Visit Details\\Facility\\') + 2, 0, 'K-O\\')
            WHEN ASCII(left(lower(c_name), 1)) BETWEEN  ASCII('p') and  ASCII('t')
            THEN insert(c_fullname,length('\\ACT\\Visit Details\\Facility\\') + 2, 0, 'P-T\\')
            WHEN ASCII(left(lower(c_name), 1)) BETWEEN  ASCII('u') and  ASCII('z')
            THEN insert(c_fullname,length('\\ACT\\Visit Details\\Facility\\') + 2, 0, 'U-Z\\')
    END,
    c_hlevel = c_hlevel + 1
where c_columnname = 'facilityid' and c_hlevel = 3;

