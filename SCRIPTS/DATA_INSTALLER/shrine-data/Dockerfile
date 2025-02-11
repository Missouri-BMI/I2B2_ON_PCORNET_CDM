# FROM mariadb:latest
FROM ubuntu:latest

ARG ENVIRONMENT=dev
ARG PROJECT=mu

RUN apt-get update && apt-get install -y wget unzip mariadb-client

# ENV SHRINE_URL=https://repo.open.catalyst.harvard.edu/nexus/content/groups/public/net/shrine/shrine-setup/3.3.2/shrine-setup-3.3.2-dist.zip
# RUN cd /home && \
#     wget $SHRINE_URL -O shrine-setup.zip && \
#     unzip shrine-setup.zip && \
#     rm shrine-setup.zip

COPY shrine-setup-3.3.2/ /home/shrine-setup/

WORKDIR /home/shrine-setup/

COPY load-scripts/${ENVIRONMENT}/${PROJECT}/*  ./load-scripts/
CMD [ "/bin/bash", "-C", "./load-scripts/001.sh" ]

# COPY /load-scripts/${ENVIRONMENT}/${PROJECT}/* /docker-entrypoint-initdb.d/