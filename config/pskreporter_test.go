package config

import (
	"reflect"
	"testing"
)

func TestPSKReporterSubscriptionTopicsWithModes(t *testing.T) {
	cfg := PSKReporterConfig{Modes: []string{"FT8", "ft4", "  "}}
	got := cfg.SubscriptionTopics()
	want := []string{"pskr/filter/v2/+/FT8/#", "pskr/filter/v2/+/FT4/#"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("SubscriptionTopics() = %v, want %v", got, want)
	}
}

func TestPSKReporterSubscriptionTopicsFallbackToTopic(t *testing.T) {
	custom := "pskr/filter/v2/+/+/#"
	cfg := PSKReporterConfig{Topic: custom}
	got := cfg.SubscriptionTopics()
	want := []string{custom}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("SubscriptionTopics() = %v, want %v", got, want)
	}
}

func TestPSKReporterSubscriptionTopicsDefault(t *testing.T) {
	cfg := PSKReporterConfig{}
	got := cfg.SubscriptionTopics()
	want := []string{defaultPSKReporterTopic}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("SubscriptionTopics() = %v, want %v", got, want)
	}
}
