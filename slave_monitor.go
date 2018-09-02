package main

import (
	"github.com/prometheus/client_golang/prometheus"
)

type (
	executor struct {
		ID          string      `json:"executor_id"`
		Name        string      `json:"executor_name"`
		FrameworkID string      `json:"framework_id"`
		Source      string      `json:"source"`
		Statistics  *statistics `json:"statistics"`
		Tasks       []task      `json:"tasks"`
	}

	statistics struct {
		Processes float64 `json:"processes"`
		Threads   float64 `json:"threads"`

		CpusLimit             float64 `json:"cpus_limit"`
		CpusSystemTimeSecs    float64 `json:"cpus_system_time_secs"`
		CpusUserTimeSecs      float64 `json:"cpus_user_time_secs"`
		CpusThrottledTimeSecs float64 `json:"cpus_throttled_time_secs"`
		CpusNrPeriods         float64 `json:"cpus_nr_periods"`
		CpusNrThrottled       float64 `json:"cpus_nr_throttled"`

		MemAnonBytes               float64 `json:"mem_anon_bytes"`
		MemLimitBytes              float64 `json:"mem_limit_bytes"`
		MemRssBytes                float64 `json:"mem_rss_bytes"`
		MemTotalBytes              float64 `json:"mem_total_bytes"`
		MemCacheBytes              float64 `json:"mem_cache_bytes"`
		MemSwapBytes               float64 `json:"mem_swap_bytes"`
		MemFileBytes               float64 `json:"mem_file_bytes"`
		MemMappedFileBytes         float64 `json:"mem_mapped_file_bytes"`
		MemUnevictableBytes        float64 `json:"mem_unevictable_bytes"`
		MemLowPressureCounter      float64 `json:"mem_low_pressure_counter"`
		MemMediumPressureCounter   float64 `json:"mem_medium_pressure_counter"`
		MemCriticalPressureCounter float64 `json:"mem_critical_pressure_counter"`

		DiskLimitBytes float64 `json:"disk_limit_bytes"`
		DiskUsedBytes  float64 `json:"disk_used_bytes"`

		NetRxBytes   float64 `json:"net_rx_bytes"`
		NetRxDropped float64 `json:"net_rx_dropped"`
		NetRxErrors  float64 `json:"net_rx_errors"`
		NetRxPackets float64 `json:"net_rx_packets"`
		NetTxBytes   float64 `json:"net_tx_bytes"`
		NetTxDropped float64 `json:"net_tx_dropped"`
		NetTxErrors  float64 `json:"net_tx_errors"`
		NetTxPackets float64 `json:"net_tx_packets"`
	}

	slaveCollector struct {
		*httpClient
		metrics map[*prometheus.Desc]metric
	}

	metric struct {
		valueType prometheus.ValueType
		get       func(*statistics) float64
		hostname  string
	}
)

func newSlaveMonitorCollector(httpClient *httpClient) prometheus.Collector {
	labels := []string{"id", "framework_id", "source", "hostname"}

	return &slaveCollector{
		httpClient: httpClient,
		metrics: map[*prometheus.Desc]metric{
			// Processes
			prometheus.NewDesc(
				"mesos_agent_processes",
				"Current number of processes",
				labels, nil,
			): metric{prometheus.GaugeValue, func(s *statistics) float64 { return s.Processes }, httpClient.hostname},
			prometheus.NewDesc(
				"mesos_agent_threads",
				"Current number of threads",
				labels, nil,
			): metric{prometheus.GaugeValue, func(s *statistics) float64 { return s.Threads }, httpClient.hostname},

			// CPU
			prometheus.NewDesc(
				"mesos_agent_cpus_limit",
				"Current limit of CPUs for task",
				labels, nil,
			): metric{prometheus.GaugeValue, func(s *statistics) float64 { return s.CpusLimit }, httpClient.hostname},
			prometheus.NewDesc(
				"mesos_agent_cpu_system_seconds_total",
				"Total system CPU seconds",
				labels, nil,
			): metric{prometheus.CounterValue, func(s *statistics) float64 { return s.CpusSystemTimeSecs }, httpClient.hostname},
			prometheus.NewDesc(
				"mesos_agent_cpu_user_seconds_total",
				"Total user CPU seconds",
				labels, nil,
			): metric{prometheus.CounterValue, func(s *statistics) float64 { return s.CpusUserTimeSecs }, httpClient.hostname},
			prometheus.NewDesc(
				"mesos_agent_cpu_throttled_seconds_total",
				"Total time CPU was throttled due to CFS bandwidth control",
				labels, nil,
			): metric{prometheus.CounterValue, func(s *statistics) float64 { return s.CpusThrottledTimeSecs }, httpClient.hostname},
			prometheus.NewDesc(
				"mesos_agent_cpu_nr_periods_total",
				"Total number of elapsed CFS enforcement intervals",
				labels, nil,
			): metric{prometheus.CounterValue, func(s *statistics) float64 { return s.CpusNrPeriods }, httpClient.hostname},
			prometheus.NewDesc(
				"mesos_agent_cpu_nr_throttled_total",
				"Total number of throttled CFS enforcement intervals.",
				labels, nil,
			): metric{prometheus.CounterValue, func(s *statistics) float64 { return s.CpusNrThrottled }, httpClient.hostname},

			// Memory
			prometheus.NewDesc(
				"mesos_agent_mem_anon_bytes",
				"Current anonymous memory in bytes",
				labels, nil,
			): metric{prometheus.GaugeValue, func(s *statistics) float64 { return s.MemAnonBytes }, httpClient.hostname},
			prometheus.NewDesc(
				"mesos_agent_mem_limit_bytes",
				"Current memory limit in bytes",
				labels, nil,
			): metric{prometheus.GaugeValue, func(s *statistics) float64 { return s.MemLimitBytes }, httpClient.hostname},
			prometheus.NewDesc(
				"mesos_agent_mem_rss_bytes",
				"Current rss memory usage",
				labels, nil,
			): metric{prometheus.GaugeValue, func(s *statistics) float64 { return s.MemRssBytes }, httpClient.hostname},
			prometheus.NewDesc(
				"mesos_agent_mem_total_bytes",
				"Current total memory usage",
				labels, nil,
			): metric{prometheus.GaugeValue, func(s *statistics) float64 { return s.MemTotalBytes }, httpClient.hostname},
			prometheus.NewDesc(
				"mesos_agent_mem_cache_bytes",
				"Current page cache memory usage",
				labels, nil,
			): metric{prometheus.GaugeValue, func(s *statistics) float64 { return s.MemCacheBytes }, httpClient.hostname},
			prometheus.NewDesc(
				"mesos_agent_mem_swap_bytes",
				"Current swap usage",
				labels, nil,
			): metric{prometheus.GaugeValue, func(s *statistics) float64 { return s.MemSwapBytes }, httpClient.hostname},
			prometheus.NewDesc(
				"mesos_agent_mem_file_bytes",
				"Current file bytes count",
				labels, nil,
			): metric{prometheus.GaugeValue, func(s *statistics) float64 { return s.MemFileBytes }, httpClient.hostname},
			prometheus.NewDesc(
				"mesos_agent_mem_mapped_file_bytes",
				"Current memory mapped file bytes count",
				labels, nil,
			): metric{prometheus.GaugeValue, func(s *statistics) float64 { return s.MemMappedFileBytes }, httpClient.hostname},
			prometheus.NewDesc(
				"mesos_agent_mem_unevictable_bytes",
				"Current memory unevictable bytes count",
				labels, nil,
			): metric{prometheus.GaugeValue, func(s *statistics) float64 { return s.MemUnevictableBytes }, httpClient.hostname},
			prometheus.NewDesc(
				"mesos_agent_mem_low_pressure_counter",
				"Low pressure counter value",
				labels, nil,
			): metric{prometheus.CounterValue, func(s *statistics) float64 { return s.MemLowPressureCounter }, httpClient.hostname},
			prometheus.NewDesc(
				"mesos_agent_mem_medium_pressure_counter",
				"Medium pressure counter value",
				labels, nil,
			): metric{prometheus.CounterValue, func(s *statistics) float64 { return s.MemMediumPressureCounter }, httpClient.hostname},
			prometheus.NewDesc(
				"mesos_agent_critical_low_pressure_counter",
				"Critical pressure counter value",
				labels, nil,
			): metric{prometheus.CounterValue, func(s *statistics) float64 { return s.MemCriticalPressureCounter }, httpClient.hostname},

			// Disk
			prometheus.NewDesc(
				"mesos_agent_disk_limit_bytes",
				"Current disk limit in bytes",
				labels, nil,
			): metric{prometheus.GaugeValue, func(s *statistics) float64 { return s.DiskLimitBytes }, httpClient.hostname},
			prometheus.NewDesc(
				"mesos_agent_disk_used_bytes",
				"Current disk usage",
				labels, nil,
			): metric{prometheus.GaugeValue, func(s *statistics) float64 { return s.DiskUsedBytes }, httpClient.hostname},

			// Network
			// - RX
			prometheus.NewDesc(
				"mesos_agent_network_receive_bytes_total",
				"Total bytes received",
				labels, nil,
			): metric{prometheus.CounterValue, func(s *statistics) float64 { return s.NetRxBytes }, httpClient.hostname},
			prometheus.NewDesc(
				"mesos_agent_network_receive_dropped_total",
				"Total packets dropped while receiving",
				labels, nil,
			): metric{prometheus.CounterValue, func(s *statistics) float64 { return s.NetRxDropped }, httpClient.hostname},
			prometheus.NewDesc(
				"mesos_agent_network_receive_errors_total",
				"Total errors while receiving",
				labels, nil,
			): metric{prometheus.CounterValue, func(s *statistics) float64 { return s.NetRxErrors }, httpClient.hostname},
			prometheus.NewDesc(
				"mesos_agent_network_receive_packets_total",
				"Total packets received",
				labels, nil,
			): metric{prometheus.CounterValue, func(s *statistics) float64 { return s.NetRxPackets }, httpClient.hostname},
			// - TX
			prometheus.NewDesc(
				"mesos_agent_network_transmit_bytes_total",
				"Total bytes transmitted",
				labels, nil,
			): metric{prometheus.CounterValue, func(s *statistics) float64 { return s.NetTxBytes }, httpClient.hostname},
			prometheus.NewDesc(
				"mesos_agent_network_transmit_dropped_total",
				"Total packets dropped while transmitting",
				labels, nil,
			): metric{prometheus.CounterValue, func(s *statistics) float64 { return s.NetTxDropped }, httpClient.hostname},
			prometheus.NewDesc(
				"mesos_agent_network_transmit_errors_total",
				"Total errors while transmitting",
				labels, nil,
			): metric{prometheus.CounterValue, func(s *statistics) float64 { return s.NetTxErrors }, httpClient.hostname},
			prometheus.NewDesc(
				"mesos_agent_network_transmit_packets_total",
				"Total packets transmitted",
				labels, nil,
			): metric{prometheus.CounterValue, func(s *statistics) float64 { return s.NetTxPackets }, httpClient.hostname},
		},
	}
}

func (c *slaveCollector) Collect(ch chan<- prometheus.Metric) {
	stats := []executor{}
	c.fetchAndDecode("/monitor/statistics", &stats)

	for _, exec := range stats {
		for desc, m := range c.metrics {
			ch <- prometheus.MustNewConstMetric(desc, m.valueType, m.get(exec.Statistics), exec.ID, exec.FrameworkID, exec.Source, m.hostname)
		}
	}
}

func (c *slaveCollector) Describe(ch chan<- *prometheus.Desc) {
	for metric := range c.metrics {
		ch <- metric
	}
}
