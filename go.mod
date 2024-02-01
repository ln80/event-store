module github.com/ln80/event-store

go 1.20

require (
	github.com/aws/aws-sdk-go-v2 v1.24.1
	github.com/aws/aws-sdk-go-v2/config v1.15.8
	github.com/aws/aws-sdk-go-v2/credentials v1.12.3
	github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue v1.9.6
	github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression v1.4.12
	github.com/aws/aws-sdk-go-v2/service/dynamodb v1.15.9
	github.com/aws/aws-sdk-go-v2/service/sns v1.21.5
	github.com/aws/smithy-go v1.19.0
	github.com/ln80/pii v0.3.0
	github.com/rs/xid v1.5.0
)

require (
	github.com/aws/aws-sdk-go-v2/feature/ec2/imds v1.12.5 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.2.10 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.5.10 // indirect
	github.com/aws/aws-sdk-go-v2/internal/ini v1.3.12 // indirect
	github.com/aws/aws-sdk-go-v2/service/appconfigdata v1.11.7 // indirect
	github.com/aws/aws-sdk-go-v2/service/dynamodbstreams v1.13.9 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.9.3 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/endpoint-discovery v1.7.8 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.9.5 // indirect
	github.com/aws/aws-sdk-go-v2/service/sso v1.11.6 // indirect
	github.com/aws/aws-sdk-go-v2/service/sts v1.16.6 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-isatty v0.0.19 // indirect
	github.com/mitchellh/copystructure v1.2.0 // indirect
	github.com/mitchellh/mapstructure v1.5.0 // indirect
	github.com/mitchellh/reflectwalk v1.0.2 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	golang.org/x/sys v0.12.0 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
)

require (
	github.com/Masterminds/semver/v3 v3.1.1
	github.com/aws/aws-sdk-go-v2/service/glue v1.72.0
	github.com/go-logr/logr v1.3.0
	github.com/go-logr/zerologr v1.2.3
	github.com/google/uuid v1.4.0
	github.com/hamba/avro/v2 v2.16.0
	github.com/rs/zerolog v1.31.0
)

replace github.com/hamba/avro/v2 => github.com/redaLaanait/avro/v2 v2.0.0-20231217171426-7db00da94c9c
