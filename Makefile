

DOCKER_NETWORK = lambda-local
DYNAMODB_PORT  = 8070
DYNAMODB_VOLUME = dynamodb-local-v2.0

start-dynamodb:
	docker run -p $(DYNAMODB_PORT):8000 amazon/dynamodb-local -jar DynamoDBLocal.jar -inMemory

start-dynamodb/persist:
	docker volume create $(DYNAMODB_VOLUME)
	docker run --rm \
	-p $(DYNAMODB_PORT):8000 \
	-v $(DYNAMODB_VOLUME):/home/dynamodblocal  \
	--network=$(DOCKER_NETWORK) \
	--name dynamodb amazon/dynamodb-local -jar DynamoDBLocal.jar \
	-sharedDb -dbPath /home/dynamodblocal

ci/test:
	go test -race -cover ./... -coverprofile coverage.out -covermode atomic

test/dynamodb: export DYNAMODB_ENDPOINT = http://localhost:$(DYNAMODB_PORT)
test/dynamodb:
	go test -race -v -cover ./dynamodb/...

test/local: test/dynamodb
	go test -race -v -cover ./...

.PHONY: examples
examples:
	go test -race -v -cover ./examples/...


include ./stack/elastic/Makefile


tool/integ-test/run:
	SCHEMA_REGISTRY_NAME=local-test-registry gotest --tags=integ -race -cover -v ./tool
