# Variables
IMAGE_NAME ?= i2b2-data-installer
TAG ?= latest

ECR_REPO_DEV := 500206249851.dkr.ecr.us-east-2.amazonaws.com
ECR_REPO_PROD := 063312575449.dkr.ecr.us-east-2.amazonaws.com

DOCKER_BUILD = docker build --platform=linux/amd64

dev:
	$(DOCKER_BUILD) --rm \
		-t $(ECR_REPO_DEV)/$(IMAGE_NAME):$(TAG) \
		.

prod:
	$(DOCKER_BUILD) --rm \
		-t $(ECR_REPO_PROD)/$(IMAGE_NAME):$(TAG) \
		.
