#!/bin/bash

docker build --platform=linux/amd64 --no-cache \
-t 063312575449.dkr.ecr.us-east-2.amazonaws.com/shrine-data-installer:mu \
--build-arg ENVIRONMENT=prod \
--build-arg PROJECT=mu \
.


# docker build --platform=linux/amd64 --no-cache \
# -t 063312575449.dkr.ecr.us-east-2.amazonaws.com/shrine-data-installer:washu \
# --build-arg ENVIRONMENT=prod \
# --build-arg PROJECT=washu \
# .