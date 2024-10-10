

ci/test:
	go test -race -cover ./... -coverprofile coverage.out -covermode atomic

local/test:
	gotest -race -v -cover ./...

.PHONY: examples
examples:
	go test -race -v -cover ./examples/...

tool%: export GOWORK=off

tool/integ-test/run:
	go test --tags=integ -race -cover -v ./tool
