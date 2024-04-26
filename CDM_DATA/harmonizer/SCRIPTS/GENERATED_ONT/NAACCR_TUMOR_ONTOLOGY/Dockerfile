FROM centos:8

COPY /environment/snowflake-snowsql-1.2.30-1.x86_64.rpm /tmp/

RUN rpm -i /tmp/snowflake-snowsql-1.2.30-1.x86_64.rpm ;\
    rm /tmp/snowflake-snowsql-1.2.30-1.x86_64.rpm

WORKDIR /home

COPY /environment/snowsql.cnf /etc/
COPY /import ./

COPY /naaccr_ontology.csv ./files/


RUN chmod 700 /etc/snowsql.cnf

CMD ["/bin/bash", "-C", "import"]