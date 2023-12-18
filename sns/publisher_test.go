package sns

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/ln80/event-store/event"
)

type event1 struct{ Val string }

func TestEventPublisher(t *testing.T) {
	ctx := context.Background()

	stmID := event.UID().String()
	topic := "http://sns-url.url"

	dest1 := "dest1"

	events := []any{}
	for i := 0; i < 15; i++ {
		events = append(events, &event1{
			Val: "test content " + strconv.Itoa(i),
		})
	}

	evtAt := time.Now()
	envs := event.Wrap(ctx, event.NewStreamID(stmID), events, func(env event.RWEnvelope) {
		env.SetAt(evtAt)
		evtAt = evtAt.Add(1 * time.Minute)
		env.SetDests([]string{dest1})
	}, event.WithGlobalVersionIncr(event.VersionMin, len(events), event.VersionSeqDiffFracPart))

	t.Run("publish with empty topic", func(t *testing.T) {
		cli := &clientMock{}
		pub := NewPublisher(cli, "")
		if err := pub.Publish(ctx, envs); err != nil {
			t.Fatalf("expect err be nil, got %v", err)
		}
		if wantL, l := 0, len(cli.traces); wantL != l {
			t.Fatalf("expect traces len be %v, got %d", wantL, l)
		}
	})

	t.Run("publish with infra error", func(t *testing.T) {
		infraError := errors.New("infra error")
		cli := &clientMock{err: infraError}
		pub := NewPublisher(cli, topic)
		if wantErr, err := infraError, pub.Publish(ctx, envs); !errors.Is(err, wantErr) {
			t.Fatalf("expect err be %v, got %v", wantErr, err)
		}
	})

	t.Run("successfully publish", func(t *testing.T) {
		cli := &clientMock{}
		pub := NewPublisher(cli, topic, func(cfg *PublisherConfig) {
			cfg.BatchRecordEnabled = false
		})
		if err := pub.Publish(ctx, envs); err != nil {
			t.Fatalf("expect err be nil, got %v", err)
		}
		if wantL, l := len(envs), len(cli.traces[topic]); wantL != l {
			t.Fatalf("expect traces len be %v, got %d", wantL, l)
		}
		if wantGrp, grp := stmID, *cli.traces[topic][2].MessageGroupId; wantGrp != grp {
			t.Fatalf("expect group ids be equals, got %s, %s", wantGrp, grp)
		}
		if wantDest, dest := dest1, aws.ToString(cli.traces[topic][2].MessageAttributes["Dests"].StringValue); wantDest != dest {
			t.Fatalf("expect dest be equals, got %s, %s", wantDest, dest)
		}
	})

	t.Run("successfully publish batch", func(t *testing.T) {
		cli := &clientMock{}
		pub := NewPublisher(cli, topic, func(cfg *PublisherConfig) {
			cfg.BatchRecordEnabled = true
		})
		if err := pub.Publish(ctx, envs); err != nil {
			t.Fatalf("expect err be nil, got %v", err)
		}
		if wantL, l := 1, len(cli.traces[topic]); wantL != l {
			t.Fatalf("expect traces len be %v, got %d", wantL, l)
		}
		if wantGrp, grp := stmID, *cli.traces[topic][0].MessageGroupId; wantGrp != grp {
			t.Fatalf("expect group ids be equals, got %s, %s", wantGrp, grp)
		}
	})

}
