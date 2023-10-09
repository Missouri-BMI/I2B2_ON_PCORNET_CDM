FROM alpine

# Download required packages
RUN apk add \
    openjdk8 \
    apache-ant \
    bash

WORKDIR /app
COPY /APP ./

CMD ["/bin/bash", "-C", "./perform_etl.sh"]