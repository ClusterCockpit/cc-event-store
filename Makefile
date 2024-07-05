TARGET = ./cc-event-store
VERSION = 0.0.1
GIT_HASH := $(shell git rev-parse --short HEAD || echo 'development')
CURRENT_TIME = $(shell date +"%Y-%m-%d:T%H:%M:%S")
LD_FLAGS = '-s -X main.date=${CURRENT_TIME} -X main.version=${VERSION} -X main.commit=${GIT_HASH}'

.PHONY: clean test swagger $(TARGET)

.NOTPARALLEL:

$(TARGET):
	$(info ===>  BUILD cc-event-store)
	@go build -ldflags=${LD_FLAGS} ./cmd/cc-event-store

cc-event-client:
	$(info ===>  BUILD cc-event-client)
	@go build -ldflags=${LD_FLAGS} ./tools/cc-event-client

swagger:
	$(info ===>  GENERATE swagger)
	@go run github.com/swaggo/swag/cmd/swag init -d ./internal/api -g api.go -o ./api
	@go run github.com/swaggo/swag/cmd/swag fmt -d ./internal/api
	@mv ./api/docs.go ./internal/api/docs.go

clean:
	$(info ===>  CLEAN)
	@go clean
	@rm -f $(TARGET)

test:
	$(info ===>  TESTING)
	@go clean -testcache
	@go build ./...
	@go vet ./...
	@go test ./...
