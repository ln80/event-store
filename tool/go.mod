module github.com/ln80/event-store/tool

go 1.23.0

require (
	github.com/aws/aws-sdk-go-v2/config v1.27.41
	github.com/hamba/avro/v2 v2.25.0
	github.com/ln80/event-store v0.0.3
	github.com/ln80/event-store.elastic v0.0.0
)

require (
	github.com/aws/aws-sdk-go-v2 v1.32.1 // indirect
	github.com/aws/aws-sdk-go-v2/credentials v1.17.39 // indirect
	github.com/aws/aws-sdk-go-v2/feature/ec2/imds v1.16.15 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.3.20 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.6.20 // indirect
	github.com/aws/aws-sdk-go-v2/internal/ini v1.8.1 // indirect
	github.com/aws/aws-sdk-go-v2/service/glue v1.77.2 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.12.0 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.12.0 // indirect
	github.com/aws/aws-sdk-go-v2/service/sso v1.24.0 // indirect
	github.com/aws/aws-sdk-go-v2/service/ssooidc v1.28.0 // indirect
	github.com/aws/aws-sdk-go-v2/service/sts v1.32.0 // indirect
	github.com/aws/smithy-go v1.22.0 // indirect
	github.com/ettle/strcase v0.2.0 // indirect
	github.com/go-logr/logr v1.4.1 // indirect
	github.com/go-logr/zerologr v1.2.3 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/ln80/struct-sensitive v0.5.0 // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/mitchellh/mapstructure v1.5.0 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/rs/xid v1.5.0 // indirect
	github.com/rs/zerolog v1.32.0 // indirect
	golang.org/x/mod v0.20.0 // indirect
	golang.org/x/sync v0.8.0 // indirect
	golang.org/x/sys v0.23.0 // indirect
	golang.org/x/tools v0.24.0 // indirect
)

replace github.com/ln80/event-store v0.0.3 => ../

replace github.com/ln80/event-store.elastic v0.0.0 => ../../event-store.elastic

replace github.com/hamba/avro/v2 v2.25.0 => ../../../hamba_avro
