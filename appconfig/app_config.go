package appconfig

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/appconfigdata"
	"github.com/aws/aws-sdk-go-v2/service/appconfigdata/types"
	"github.com/ln80/event-store/control"
)

type LoaderConfig struct {
	Application   string
	Configuration string
	Environment   string

	MinimumPollInterval time.Duration
}

func (lc LoaderConfig) IsZero() bool {
	return lc == LoaderConfig{}
}

type Loader struct {
	api ClientAPI

	cfg *LoaderConfig

	input atomic.Value // appconfigdata.GetLatestConfigurationInput
}

var _ control.Loader = &Loader{}

func NewLoader(ctx context.Context, api ClientAPI, opts ...func(*LoaderConfig)) (*Loader, error) {
	l := &Loader{
		api: api,
		cfg: &LoaderConfig{
			MinimumPollInterval: 25 * time.Second,
		},
	}

	for _, opt := range opts {
		if opt == nil {
			continue
		}
		opt(l.cfg)
	}
	if l.cfg.IsZero() {
		return nil, errors.New("empty appconfig loader parameters")
	}

	out, err := l.api.StartConfigurationSession(ctx, &appconfigdata.StartConfigurationSessionInput{
		ApplicationIdentifier:                aws.String(l.cfg.Application),
		EnvironmentIdentifier:                aws.String(l.cfg.Environment),
		ConfigurationProfileIdentifier:       aws.String(l.cfg.Configuration),
		RequiredMinimumPollIntervalInSeconds: aws.Int32(int32(l.cfg.MinimumPollInterval.Seconds())),
	})
	if err != nil {
		return nil, err
	}

	l.input.Store(appconfigdata.GetLatestConfigurationInput{
		ConfigurationToken: out.InitialConfigurationToken,
	})

	return l, nil
}

func (l *Loader) Load(ctx context.Context) ([]byte, error) {
	input := l.input.Load().(appconfigdata.GetLatestConfigurationInput)
	out, err := l.api.GetLatestConfiguration(ctx, &input)
	if err != nil {
		var te *types.BadRequestException
		if errors.As(err, &te) && strings.Contains(te.Error(), "Request too early") {
			return nil, nil
		}
		return nil, err
	}

	l.input.Store(appconfigdata.GetLatestConfigurationInput{
		ConfigurationToken: out.NextPollConfigurationToken,
	})

	return out.Configuration, nil
}
