VERSION_MAJOR=1
VERSION_MINOR=0
VERSION_PATCH=0
COMMIT := $(shell git log -n 1 --pretty=format:%h)
VERSION=$(VERSION_MAJOR).$(VERSION_MINOR).$(VERSION_PATCH)-$(COMMIT)

DOCKER_VERSION=$(VERSION_MAJOR).$(VERSION_MINOR).$(VERSION_PATCH)
DOCKER_REPO=elastisys/kube-insight-logserver

# built executable binaries are placed here
BIN_DIR=bin
# build artefacts placed here
BUILD_DIR=build
# test coverage placed here
COVER_DIR=$(BUILD_DIR)/coverage

build: dep test
	mkdir -p $(BIN_DIR)/
	go build -ldflags "-X main.version=$(VERSION)" \
	 -o $(BIN_DIR)/kube-insight-logserver ./cmd/kube-insight-logserver/

alpine: dep test
	mkdir -p $(BIN_DIR)/
	CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo \
	  -ldflags "-X main.version=$(VERSION)" \
	  -o $(BIN_DIR)/kube-insight-logserver-alpine ./cmd/kube-insight-logserver/

dep:
	dep ensure

test: dep
	mkdir -p $(COVER_DIR)
	go test -cover -coverprofile=$(COVER_DIR)/coverage.txt ./pkg/... $(TEST_ARGS)

docker-image: alpine
	docker build --tag=$(DOCKER_REPO):$(DOCKER_VERSION) .

docker-push: docker-image
	docker push $(DOCKER_REPO):$(DOCKER_VERSION)

clean:
	find -name '*~' -exec rm {} \;
	rm -rf $(BIN_DIR) $(BUILD_DIR)
