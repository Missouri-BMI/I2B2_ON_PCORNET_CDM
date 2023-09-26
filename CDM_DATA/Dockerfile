FROM alpine:3.14

# Download required packages
RUN apk add \
    openjdk8 \
    apache-ant

WORKDIR /app
COPY /APP ./

CMD ["/bin/bash", "-C", "./perform_etl.sh"]