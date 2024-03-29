
.PHONY: validate, build, package, publish, test, stackname, elastic

TEST_ENV = test
TEST_REGION = eu-west-1

export AWS_REGION=$(TEST_REGION)

# stack name can be generated by CI
ifndef TEST_STACK_NAME
override TEST_STACK_NAME = elastic-event-store-integ-test
endif

# Event store name
TEST_PARAMS = Name=$(TEST_STACK_NAME)


ELASTIC_DIR := $(dir $(lastword $(MAKEFILE_LIST)))

# elastic related build and test of should ignore parent workspace
elastic%: export GOWORK=off

elastic/validate:
	cd $(ELASTIC_DIR); \
	sam validate

elastic/build:
	cd $(ELASTIC_DIR); \
	v="`go list -m -f '{{ .Version }}' github.com/ln80/event-store`"; \
	# if [  "$v" == "" ]; then echo "module version not found" ; exit 2; fi; \
	echo "Go module version: $$v"; \
	sam build --template-file ./template.yaml

elastic/package:
	cd $(ELASTIC_DIR); \
	sam package --output-template-file packaged.yaml --s3-bucket ln80-sam-pkgs

elastic/publish:
	cd $(ELASTIC_DIR); \
	if [  "$v" == "" ]; \
	then \
		sam publish --template packaged.yaml  --fail-on-same-version   --region $(AWS_REGION); \
	else \
		sam publish --template packaged.yaml  --semantic-version $(v) --fail-on-same-version  --region $(AWS_REGION); \
	fi

elastic/unit-test/run: 
	cd $(ELASTIC_DIR); \
	echo GOWORK=$(GOWORK); \
	go test --tags=unit -race -cover -v ./...

elastic/integ-test/stackname:
	cd $(ELASTIC_DIR); \
	echo $(TEST_STACK_NAME)

elastic/integ-test/build:
	cd $(ELASTIC_DIR); \
	sam build --template-file ./integ-test-template.yaml
	
elastic/integ-test/deploy:
	cd $(ELASTIC_DIR); \
	sam deploy \
		--no-confirm-changeset \
		--no-fail-on-empty-changeset \
		--stack-name $(TEST_STACK_NAME) \
		--config-env $(TEST_ENV) \
		--capabilities CAPABILITY_IAM CAPABILITY_AUTO_EXPAND CAPABILITY_NAMED_IAM \
		--region $(TEST_REGION) \
		--resolve-s3

elastic/integ-test/clear:
	cd $(ELASTIC_DIR); \
	sam delete --no-prompts --stack-name $(TEST_STACK_NAME) --region $(TEST_REGION)

elastic/integ-test/run: export STACK_NAME=$(TEST_STACK_NAME)
elastic/integ-test/run:
	cd $(ELASTIC_DIR); \
	go test --tags=integ -race -cover -v 

elastic/integ-test/logs:
	cd $(ELASTIC_DIR); \
	sam logs --stack-name $(TEST_STACK_NAME) --region $(TEST_REGION) --tail


elastic/integ-test/xray:
	docker run --rm \
		--env AWS_ACCESS_KEY_ID=$$(aws configure get aws_access_key_id) \
		--env AWS_SECRET_ACCESS_KEY=$$(aws configure get aws_secret_access_key) \
		--env AWS_REGION=eu-west-1 \
		--name xray-daemon \
		--publish 2000:2000/udp \
		amazon/aws-xray-daemon -o