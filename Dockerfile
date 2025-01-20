FROM apache/airflow:2.10.0-python3.12

USER root

RUN apt-get update && \
    apt-get install -y \
        openjdk-17-jdk \
        zip unzip


ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64/
ENV PATH=$PATH:$JAVA_HOME/bin

USER airflow

ADD requirements.txt .
RUN pip install apache-airflow==${AIRFLOW_VERSION} -r requirements.txt