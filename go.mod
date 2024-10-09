module github.com/ln80/event-store

go 1.23.0

require (
	github.com/aws/aws-sdk-go-v2 v1.32.1
	github.com/aws/aws-sdk-go-v2/config v1.27.41
	github.com/aws/aws-sdk-go-v2/credentials v1.17.39 // indirect
	github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue v1.15.11
	github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression v1.7.46
	github.com/aws/aws-sdk-go-v2/service/dynamodb v1.36.1
	github.com/aws/aws-sdk-go-v2/service/sns v1.26.3
	github.com/aws/smithy-go v1.22.0
	github.com/rs/xid v1.5.0
)

require (
	github.com/andybalholm/brotli v1.0.6 // indirect
	github.com/aws/aws-sdk-go v1.47.9 // indirect
	github.com/aws/aws-sdk-go-v2/feature/ec2/imds v1.16.15 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.3.20 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.6.20 // indirect
	github.com/aws/aws-sdk-go-v2/internal/ini v1.8.1 // indirect
	github.com/aws/aws-sdk-go-v2/service/dynamodbstreams v1.24.1 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.12.0 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/endpoint-discovery v1.10.1 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.12.0 // indirect
	github.com/aws/aws-sdk-go-v2/service/sso v1.24.0 // indirect
	github.com/aws/aws-sdk-go-v2/service/ssooidc v1.28.0 // indirect
	github.com/aws/aws-sdk-go-v2/service/sts v1.32.0 // indirect
	github.com/golang/protobuf v1.5.3 // indirect
	github.com/google/go-cmp v0.6.0 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/klauspost/compress v1.17.9 // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/mitchellh/mapstructure v1.5.0 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/valyala/bytebufferpool v1.0.0 // indirect
	github.com/valyala/fasthttp v1.50.0 // indirect
	golang.org/x/net v0.28.0 // indirect
	golang.org/x/sys v0.23.0 // indirect
	golang.org/x/text v0.17.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20231120223509-83a465c0220f // indirect
	google.golang.org/grpc v1.59.0 // indirect
	google.golang.org/protobuf v1.31.0 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
)

require (
	github.com/Masterminds/semver/v3 v3.1.1
	github.com/aws/aws-sdk-go-v2/service/appconfigdata v1.11.7
	github.com/aws/aws-sdk-go-v2/service/glue v1.77.2
	github.com/aws/aws-xray-sdk-go v1.8.3
	github.com/go-logr/logr v1.4.1
	github.com/go-logr/zerologr v1.2.3
	github.com/google/uuid v1.6.0
	github.com/hamba/avro/v2 v2.25.0
	github.com/ln80/aws-toolkit-go v0.1.0
	github.com/ln80/privacy-engine v0.0.0-20240921175436-c37fd501bf36
	github.com/ln80/struct-sensitive v0.5.0
	github.com/modern-go/reflect2 v1.0.2
	github.com/rs/zerolog v1.32.0
)

replace github.com/ln80/aws-toolkit-go v0.1.0 => ../aws-toolkit-go
