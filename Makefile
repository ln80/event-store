

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

test/ci:
	go test -race -cover ./...

test/dynamodb: export DYNAMODB_ENDPOINT = http://localhost:$(DYNAMODB_PORT)
test/dynamodb:
	gotest -race -v -cover ./dynamodb/...

test/local: test/dynamodb
	gotest -race -v -cover ./...

PHONY: examples
examples:
	gotest -race -v -cover ./examples/...


include ./stack/elastic/Makefile