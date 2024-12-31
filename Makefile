VERSION = 0.0.1
GIT_HASH := $(shell git rev-parse --short HEAD || echo 'development')
CURRENT_TIME = $(shell date +"%Y-%m-%d:T%H:%M:%S")
LD_FLAGS = '-s -X main.date=${CURRENT_TIME} -X main.version=${VERSION} -X main.commit=${GIT_HASH}'

.PHONY: clean test swagger cc-event-store cc-event-client

.NOTPARALLEL:

all: cc-event-store cc-event-client

cc-event-store: ./cmd/cc-event-store/cc-event-store.go ./cmd/cc-event-store/router.go
	$(info ===>  BUILD cc-event-store)
	@go build -ldflags=${LD_FLAGS} ./cmd/cc-event-store

cc-event-client: cmd/cc-event-client/cc-event-client.go
	$(info ===>  BUILD cc-event-client)
	@go build -ldflags=${LD_FLAGS} ./cmd/cc-event-client

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
