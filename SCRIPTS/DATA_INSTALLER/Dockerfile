FROM amazonlinux:2023

# Install Java (OpenJDK 17) and PostgreSQL client (psql)
RUN dnf update -y && \
    dnf install -y java-17-amazon-corretto-devel postgresql15 && \
    dnf clean all

# Set JAVA_HOME environment variable
ENV JAVA_HOME=/usr/lib/jvm/java-17-amazon-corretto
ENV PATH=$JAVA_HOME/bin:$PATH

WORKDIR /tmp

COPY i2b2-data/edu.harvard.i2b2.data ./i2b2-data/edu.harvard.i2b2.data
COPY install_postgresql.sh ./install_postgresql.sh

RUN chmod +x ./install_postgresql.sh

ENTRYPOINT ["/bin/bash", "-c"]

CMD ["./install_postgresql.sh"]
