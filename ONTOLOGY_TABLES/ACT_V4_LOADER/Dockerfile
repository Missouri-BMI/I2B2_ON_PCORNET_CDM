FROM centos:8

COPY /conf/snowflake-snowsql-1.2.31-1.x86_64.rpm /tmp/

RUN rpm -i /tmp/snowflake-snowsql-1.2.31-1.x86_64.rpm ;\
    rm /tmp/snowflake-snowsql-1.2.31-1.x86_64.rpm

WORKDIR /home

COPY /conf/snowsql.cnf /etc/

COPY scripts/ ./scripts/

RUN chmod -R 755 ./scripts


CMD [ "./scripts/act-snowflake" ]