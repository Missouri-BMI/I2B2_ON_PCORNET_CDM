BEGIN
    USE SCHEMA REPORT;

    call generate_orphan_concepts();
    call generate_ontology_concepts_with_path();
    call generate_cdm_gaps();
END;
