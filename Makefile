REGISTRY_NAME?=docker.io/iejalapeno
IMAGE_VERSION?=latest

.PHONY: all ebgp-processor container push clean test

ifdef V
TESTARGS = -v -args -alsologtostderr -v 5
else
TESTARGS =
endif

all: ebgp-processor

ebgp-processor:
	mkdir -p bin
	$(MAKE) -C ./cmd compile-ebgp-processor

ebgp-processor-container: ebgp-processor
	docker build -t $(REGISTRY_NAME)/ebgp-processor:$(IMAGE_VERSION) -f ./build/Dockerfile.ebgp-processor .

push: ebgp-processor-container
	docker push $(REGISTRY_NAME)/ebgp-processor:$(IMAGE_VERSION)

clean:
	rm -rf bin

test:
	GO111MODULE=on go test `go list ./... | grep -v 'vendor'` $(TESTARGS)
	GO111MODULE=on go vet `go list ./... | grep -v vendor`
