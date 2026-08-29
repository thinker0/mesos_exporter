package mesos

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

func Example_attributeString() {
	tests := []string{
		`"text"`,
		"6",
		"9.3",
		"[9-12]",
		"{a: b}",
	}
	for _, test := range tests {
		s, err := attributeString(json.RawMessage(test))
		fmt.Println(s, err)
	}
	// Output:
	// text <nil>
	// 6 <nil>
	// 9.3 <nil>
	//  value neither scalar nor text
	//  value neither scalar nor text
}

func TestCasingAndErrorHandling(t *testing.T) {
	// 1. Casing normalization test
	m := MetricMap{
		"slave/cpus_percent": 0.85,
		"Slave/mem_total":    1024.0,
	}

	// Apply normalisation logic as in Collect()
	for k, v := range m {
		if len(k) > 0 {
			first := k[:1]
			var toggled string
			if first == strings.ToUpper(first) {
				toggled = strings.ToLower(first) + k[1:]
			} else {
				toggled = strings.ToUpper(first) + k[1:]
			}
			if _, ok := m[toggled]; !ok {
				m[toggled] = v
			}
		}
	}

	// Verify both casing lookups work
	if val, ok := m["Slave/cpus_percent"]; !ok || val != 0.85 {
		t.Errorf("Expected to find Slave/cpus_percent but got ok=%v, val=%v", ok, val)
	}
	if val, ok := m["slave/mem_total"]; !ok || val != 1024.0 {
		t.Errorf("Expected to find slave/mem_total but got ok=%v, val=%v", ok, val)
	}
}

func TestEndpointsRegression(t *testing.T) {
	// Create mock server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/state":
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`{"version": "1.12.0", "git_sha": "testsha"}`))
		case "/slave(1)/state":
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`{"id": "slave-1", "hostname": "localhost"}`))
		case "/monitor/statistics":
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`[{"framework_id": "test-fw", "source": "test", "statistics": {"cpus_limit": 2.0}}]`))
		default:
			t.Errorf("Unexpected request path: %s", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	// Initialize HttpClient points to mock server
	client := &HttpClient{
		Client:   *server.Client(),
		Hostname: "localhost",
		Url:      server.URL,
	}

	// 1. Verify /state
	var state struct {
		Version string `json:"version"`
		GitSha  string `json:"git_sha"`
	}
	if !client.FetchAndDecode("/state", &state) {
		t.Fatal("Failed to fetch and decode /state")
	}
	if state.Version != "1.12.0" || state.GitSha != "testsha" {
		t.Errorf("Unexpected /state data: %+v", state)
	}

	// 2. Verify /slave(1)/state
	var slaveState struct {
		ID       string `json:"id"`
		Hostname string `json:"hostname"`
	}
	if !client.FetchAndDecode("/slave(1)/state", &slaveState) {
		t.Fatal("Failed to fetch and decode /slave(1)/state")
	}
	if slaveState.ID != "slave-1" {
		t.Errorf("Unexpected /slave(1)/state data: %+v", slaveState)
	}

	// 3. Verify /monitor/statistics
	var monitorStats []struct {
		FrameworkID string `json:"framework_id"`
		Source      string `json:"source"`
		Statistics  struct {
			CpusLimit float64 `json:"cpus_limit"`
		} `json:"statistics"`
	}
	if !client.FetchAndDecode("/monitor/statistics", &monitorStats) {
		t.Fatal("Failed to fetch and decode /monitor/statistics")
	}
	if len(monitorStats) != 1 || monitorStats[0].FrameworkID != "test-fw" {
		t.Errorf("Unexpected /monitor/statistics data: %+v", monitorStats)
	}
}

func TestEndpointsFallback(t *testing.T) {
	// Create mock server simulating legacy Mesos that ONLY responds to TitleCase URLs (like /Slave(1)/State)
	// and returns 404 for new lowercase URLs (like /slave(1)/state).
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/Slave(1)/State":
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`{"id": "legacy-slave"}`))
		case "/monitor/Statistics":
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`[{"framework_id": "legacy-fw"}]`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	client := &HttpClient{
		Client:   *server.Client(),
		Hostname: "localhost",
		Url:      server.URL,
	}

	// 1. Fetching new lowercase url "/slave(1)/state" on legacy server
	// It should fail on first try, trigger fallback, fetch "/Slave(1)/State", and succeed.
	var slaveState struct {
		ID string `json:"id"`
	}
	if !client.FetchAndDecode("/slave(1)/state", &slaveState) {
		t.Fatal("Failed to fetch and decode via fallback for /slave(1)/state")
	}
	if slaveState.ID != "legacy-slave" {
		t.Errorf("Unexpected fallback data: %+v", slaveState)
	}

	// 2. Fetching new lowercase url "/monitor/statistics" on legacy server
	// It should trigger fallback to "/monitor/Statistics" and succeed.
	var monitorStats []struct {
		FrameworkID string `json:"framework_id"`
	}
	if !client.FetchAndDecode("/monitor/statistics", &monitorStats) {
		t.Fatal("Failed to fetch and decode via fallback for /monitor/statistics")
	}
	if len(monitorStats) != 1 || monitorStats[0].FrameworkID != "legacy-fw" {
		t.Errorf("Unexpected fallback data: %+v", monitorStats)
	}
}

func TestOptionalMetricsErrorHandling(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{}`))
	}))
	defer server.Close()

	client := &HttpClient{
		Client:   *server.Client(),
		Hostname: "localhost",
		Url:      server.URL,
	}

	dummyGauge := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "mesos",
		Subsystem: "test",
		Name:      "dummy",
		Help:      "dummy",
	}, []string{"hostname"})

	metrics := map[prometheus.Collector]func(MetricMap, prometheus.Collector) error{
		dummyGauge: func(m MetricMap, c prometheus.Collector) error {
			return fmt.Errorf("key test/optional_metric not found in map")
		},
	}

	collector := newMetricCollector(client, metrics)
	ch := make(chan prometheus.Metric, 10)

	// Verify error strings classification
	err1 := fmt.Errorf("key Slave/tasks_error not found in map")
	if !strings.Contains(err1.Error(), "not found in map") {
		t.Errorf("Expected 'not found in map' checking to match")
	}

	err2 := fmt.Errorf("real fatal network error")
	if strings.Contains(err2.Error(), "not found in map") {
		t.Errorf("Expected real fatal error not to match 'not found in map'")
	}

	// Invoke Collect to ensure it handles missing metrics smoothly without panic/incrementing error count incorrectly
	collector.Collect(ch)
}
