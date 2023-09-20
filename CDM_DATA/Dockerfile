# Use the CentOS 7 base image
FROM centos/python-38-centos7


# Install Snowflake Connector
RUN pip install "snowflake-connector-python[pandas]"

# Install Java
RUN yum -y update && yum clean all

RUN yum -y install  java-1.8.0-openjdk

# Install Apache Ant
RUN yum -y install ant

# Set environment variables
ENV JAVA_HOME /usr/lib/jvm/java-1.8.0-openjdk
ENV ANT_HOME /usr/share/ant

# Add Ant and Java to the PATH
ENV PATH $PATH:$JAVA_HOME/bin:$ANT_HOME/bin


WORKDIR /app
COPY /perform_etl.sh ./
COPY /CDM_ETL ./CDM_ETL
COPY /CDM_COUNT ./CDM_COUNT
COPY /MISSING_OBS ./MISSING_OBS


CMD ["/bin/bash", "-C", "./perform_etl.sh"]