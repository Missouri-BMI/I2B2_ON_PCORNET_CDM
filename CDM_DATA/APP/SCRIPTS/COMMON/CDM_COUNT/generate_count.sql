BEGIN
    use schema i2b2metadata;
    call run_on_all_fact();
END;