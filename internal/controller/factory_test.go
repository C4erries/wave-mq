package controller

import (
	"testing"

	"github.com/c4erries/wave-mq/internal/metadata"
	"github.com/c4erries/wave-mq/pkg/api"
)

func TestNewControllerDefaultsToSingleMode(t *testing.T) {
	t.Parallel()

	ctrl, err := NewController(api.BrokerConfig{BrokerID: 1}, map[string]metadata.TopicState{})
	if err != nil {
		t.Fatalf("NewController() error: %v", err)
	}

	if _, ok := ctrl.(*SingleNodeController); !ok {
		t.Fatalf("expected *SingleNodeController, got %T", ctrl)
	}
}

func TestNewControllerRejectsUnknownMode(t *testing.T) {
	t.Parallel()

	_, err := NewController(api.BrokerConfig{BrokerID: 1, ControllerMode: "unknown"}, map[string]metadata.TopicState{})
	if err == nil {
		t.Fatal("expected error for unknown controller mode")
	}
}
