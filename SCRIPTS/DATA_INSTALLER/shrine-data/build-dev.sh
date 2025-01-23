#!/bin/bash

docker build --platform=linux/amd64  \
-t 500206249851.dkr.ecr.us-east-2.amazonaws.com/shrine-data-installer:mu \
--build-arg ENVIRONMENT=dev \
--build-arg PROJECT=mu \
.


# docker build --platform=linux/amd64 --no-cache \
# -t 500206249851.dkr.ecr.us-east-2.amazonaws.com/shrine-data-installer:washu \
# --build-arg ENVIRONMENT=dev \
# --build-arg PROJECT=washu \
# .