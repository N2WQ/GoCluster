package config

import (
	"reflect"
	"testing"
)

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
