FROM centos:8

COPY snowflake-snowsql-1.2.27-1.x86_64.rpm /tmp/
RUN rpm -i /tmp/snowflake-snowsql-1.2.27-1.x86_64.rpm ;\
    rm /tmp/snowflake-snowsql-1.2.27-1.x86_64.rpm

WORKDIR /home
COPY /files/ ./files/

COPY /snowsql.cnf /etc/
COPY /import ./
# COPY /create_i2b2metadata_tables.sql ./

CMD ["/bin/bash", "-C", "import"]