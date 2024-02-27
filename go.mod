module code.cloudfoundry.org/rep

go 1.22.0

replace github.com/cloudfoundry-incubator/executor v0.0.0 => code.cloudfoundry.org/executor v0.0.0

require (
	code.cloudfoundry.org/archiver v0.0.0-20240216143500-7a92f5fdc163
	code.cloudfoundry.org/bbs v0.0.0-20240208160729-6d10e764fb3e
	code.cloudfoundry.org/cfhttp v2.0.0+incompatible
	code.cloudfoundry.org/cfhttp/v2 v2.0.0
	code.cloudfoundry.org/clock v1.1.0
	code.cloudfoundry.org/debugserver v0.0.0-20240216143506-a6177cebb9a9
	code.cloudfoundry.org/diego-logging-client v0.0.0-20240223143657-dcc89d577b62
	code.cloudfoundry.org/durationjson v0.0.0-20240216143501-1b50cf8f87bc
	code.cloudfoundry.org/ecrhelper v0.0.0-20230612152651-56a9cdc53d60
	code.cloudfoundry.org/executor v0.1442.0
	code.cloudfoundry.org/garden v0.0.0-20240214130550-8a0cb81e0f4f
	code.cloudfoundry.org/go-loggregator/v8 v8.0.5
	code.cloudfoundry.org/inigo v0.0.0-20240208170101-8429a5ff895e
	code.cloudfoundry.org/lager/v3 v3.0.3
	code.cloudfoundry.org/localip v0.0.0-20240216143504-65247cb8fdb0
	code.cloudfoundry.org/locket v0.0.0-20231220192941-f252282ff31f
	code.cloudfoundry.org/operationq v0.0.0-20230612152908-b625ea3c1931
	code.cloudfoundry.org/routing-info v0.0.0-20230911184850-3a6d4ccb3cfc
	code.cloudfoundry.org/tlsconfig v0.0.0-20240216143505-4f8d9b753d56
	github.com/hashicorp/go-multierror v1.1.1
	github.com/nu7hatch/gouuid v0.0.0-20131221200532-179d4d0c4d8d
	github.com/onsi/ginkgo/v2 v2.15.0
	github.com/onsi/gomega v1.31.1
	github.com/tedsuo/ifrit v0.0.0-20230516164442-7862c310ad26
	github.com/tedsuo/rata v1.0.0
	google.golang.org/grpc v1.62.0
)

require (
	code.cloudfoundry.org/bytefmt v0.0.0-20231017140541-3b893ed0421b // indirect
	code.cloudfoundry.org/cacheddownloader v0.0.0-20240214143422-a406e3779b66 // indirect
	code.cloudfoundry.org/dockerdriver v0.0.0-20240213153304-5bf6621f54e1 // indirect
	code.cloudfoundry.org/eventhub v0.0.0-20240216143504-715392208175 // indirect
	code.cloudfoundry.org/go-diodes v0.0.0-20240220215648-1478b399ee36 // indirect
	code.cloudfoundry.org/goshims v0.30.0 // indirect
	code.cloudfoundry.org/volman v0.0.0-20230612151341-b60663cd44e0 // indirect
	code.cloudfoundry.org/workpool v0.0.0-20230612151832-b93da105e0e8 // indirect
	filippo.io/edwards25519 v1.1.0 // indirect
	github.com/aws/aws-sdk-go v1.50.26 // indirect
	github.com/aws/aws-sdk-go-v2 v1.24.1 // indirect
	github.com/aws/aws-sdk-go-v2/config v1.26.6 // indirect
	github.com/aws/aws-sdk-go-v2/credentials v1.16.16 // indirect
	github.com/aws/aws-sdk-go-v2/feature/ec2/imds v1.14.11 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.2.10 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.5.10 // indirect
	github.com/aws/aws-sdk-go-v2/internal/ini v1.7.3 // indirect
	github.com/aws/aws-sdk-go-v2/service/ecr v1.24.7 // indirect
	github.com/aws/aws-sdk-go-v2/service/ecrpublic v1.21.6 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.10.4 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.10.10 // indirect
	github.com/aws/aws-sdk-go-v2/service/sso v1.18.7 // indirect
	github.com/aws/aws-sdk-go-v2/service/ssooidc v1.21.7 // indirect
	github.com/aws/aws-sdk-go-v2/service/sts v1.26.7 // indirect
	github.com/aws/smithy-go v1.19.0 // indirect
	github.com/awslabs/amazon-ecr-credential-helper/ecr-login v0.0.0-20240206212017-5795caca6e8e // indirect
	github.com/bmizerany/pat v0.0.0-20210406213842-e4b6760bdd6f // indirect
	github.com/census-instrumentation/opencensus-proto v0.4.1 // indirect
	github.com/cncf/xds/go v0.0.0-20231128003011-0fa0005c9caa // indirect
	github.com/cockroachdb/apd v1.1.0 // indirect
	github.com/cyphar/filepath-securejoin v0.2.4 // indirect
	github.com/envoyproxy/go-control-plane v0.12.0 // indirect
	github.com/envoyproxy/protoc-gen-validate v1.0.4 // indirect
	github.com/ghodss/yaml v1.0.0 // indirect
	github.com/go-logr/logr v1.4.1 // indirect
	github.com/go-sql-driver/mysql v1.7.1 // indirect
	github.com/go-task/slim-sprig v0.0.0-20230315185526-52ccab3ef572 // indirect
	github.com/go-test/deep v1.1.0 // indirect
	github.com/gofrs/uuid v4.4.0+incompatible // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/protobuf v1.5.3 // indirect
	github.com/google/go-cmp v0.6.0 // indirect
	github.com/google/pprof v0.0.0-20240207164012-fb44976bdcd5 // indirect
	github.com/google/shlex v0.0.0-20191202100458-e7afc7fbc510 // indirect
	github.com/hashicorp/errwrap v1.1.0 // indirect
	github.com/jackc/fake v0.0.0-20150926172116-812a484cc733 // indirect
	github.com/jackc/pgx v3.6.2+incompatible // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/lib/pq v1.10.9 // indirect
	github.com/mitchellh/go-homedir v1.1.0 // indirect
	github.com/openzipkin/zipkin-go v0.4.2 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/sirupsen/logrus v1.9.3 // indirect
	github.com/square/certstrap v1.3.0 // indirect
	github.com/vito/go-sse v1.0.0 // indirect
	go.step.sm/crypto v0.43.1 // indirect
	golang.org/x/crypto v0.19.0 // indirect
	golang.org/x/net v0.21.0 // indirect
	golang.org/x/sys v0.17.0 // indirect
	golang.org/x/text v0.14.0 // indirect
	golang.org/x/time v0.5.0 // indirect
	golang.org/x/tools v0.18.0 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20240125205218-1f4bbc51befe // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20240221002015-b0ce06bbee7c // indirect
	google.golang.org/protobuf v1.32.0 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
