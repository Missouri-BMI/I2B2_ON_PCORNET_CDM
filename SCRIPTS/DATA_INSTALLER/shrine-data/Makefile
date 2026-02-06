# Variables
DEV_REGISTRY = 500206249851.dkr.ecr.us-east-2.amazonaws.com
PROD_REGISTRY = 063312575449.dkr.ecr.us-east-2.amazonaws.com
IMAGE_NAME = shrine-data-installer
PLATFORM = linux/amd64

# Default target
.PHONY: dev-mu dev-washu prod-mu prod-washu

# Development builds
dev-mu:
	docker build --platform=$(PLATFORM) \
		-t $(DEV_REGISTRY)/$(IMAGE_NAME):mu \
		--build-arg ENVIRONMENT=dev \
		--build-arg PROJECT=mu \
		.

dev-washu:
	docker build --platform=$(PLATFORM) \
		-t $(DEV_REGISTRY)/$(IMAGE_NAME):washu \
		--build-arg ENVIRONMENT=dev \
		--build-arg PROJECT=washu \
		.

# Production builds
prod-mu:
	docker build --platform=$(PLATFORM) \
		-t $(PROD_REGISTRY)/$(IMAGE_NAME):mu \
		--build-arg ENVIRONMENT=prod \
		--build-arg PROJECT=mu \
		.

prod-washu:
	docker build --platform=$(PLATFORM)  \
		-t $(PROD_REGISTRY)/$(IMAGE_NAME):washu \
		--build-arg ENVIRONMENT=prod \
		--build-arg PROJECT=washu \
		.
