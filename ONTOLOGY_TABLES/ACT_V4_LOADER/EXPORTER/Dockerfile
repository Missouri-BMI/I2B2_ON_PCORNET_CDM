FROM centos:8

COPY snowflake-snowsql-1.2.27-1.x86_64.rpm /tmp/
RUN rpm -i /tmp/snowflake-snowsql-1.2.27-1.x86_64.rpm ;\
    rm /tmp/snowflake-snowsql-1.2.27-1.x86_64.rpm

WORKDIR /home

COPY /snowsql.cnf /etc/
COPY /export ./
RUN mkdir ./stage
CMD ["/bin/bash", "-C", "export"]