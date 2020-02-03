package main

import (
	"encoding/json"
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
		valueType   prometheus.ValueType
		get         func(*statistics) float64
		labelNames  []string
		labelValues prometheus.Labels
	}
)

func newSlaveMonitorCollector(httpClient *httpClient, attr map[string]json.RawMessage, userTaskLabelList []string, slaveAttributeLabelList []string) prometheus.Collector {
	labels := []string{"framework_id", "source"}
	addLabels := append(userTaskLabelList, slaveAttributeLabelList...)
	labelNames := append(labels, normaliseLabelList(addLabels)...)
	attrLabels := prometheus.Labels{}
	for _, label := range labelNames {
		attrLabels[label] = ""
	}
	for key, value := range attr {
		normalisedLabel := normaliseLabel(key)
		if stringInSlice(normalisedLabel, labelNames) {
			if attribute, err := attributeString(value); err == nil {
				attrLabels[normalisedLabel] = attribute
			}
		}
	}

	return &slaveCollector{
		httpClient: httpClient,
		metrics: map[*prometheus.Desc]metric{
			// Processes
			prometheus.NewDesc(
				"mesos_agent_processes",
				"Current number of processes",
				labelNames, nil,
			): metric{prometheus.GaugeValue,
				func(s *statistics) float64 { return s.Processes },
				labelNames, attrLabels},
			prometheus.NewDesc(
				"mesos_agent_threads",
				"Current number of threads",
				labelNames, nil,
			): metric{prometheus.GaugeValue, func(s *statistics) float64 { return s.Threads },
				labelNames, attrLabels},

			// CPU
			prometheus.NewDesc(
				"mesos_agent_cpus_limit",
				"Current limit of CPUs for task",
				labelNames, nil,
			): metric{prometheus.GaugeValue, func(s *statistics) float64 { return s.CpusLimit },
				labelNames, attrLabels},
			prometheus.NewDesc(
				"mesos_agent_cpu_system_seconds_total",
				"Total system CPU seconds",
				labelNames, nil,
			): metric{prometheus.CounterValue, func(s *statistics) float64 { return s.CpusSystemTimeSecs },
				labelNames, attrLabels},
			prometheus.NewDesc(
				"mesos_agent_cpu_user_seconds_total",
				"Total user CPU seconds",
				labelNames, nil,
			): metric{prometheus.CounterValue, func(s *statistics) float64 { return s.CpusUserTimeSecs },
				labelNames, attrLabels},
			prometheus.NewDesc(
				"mesos_agent_cpu_throttled_seconds_total",
				"Total time CPU was throttled due to CFS bandwidth control",
				labelNames, nil,
			): metric{prometheus.CounterValue, func(s *statistics) float64 { return s.CpusThrottledTimeSecs },
				labelNames, attrLabels},
			prometheus.NewDesc(
				"mesos_agent_cpu_nr_periods_total",
				"Total number of elapsed CFS enforcement intervals",
				labelNames, nil,
			): metric{prometheus.CounterValue, func(s *statistics) float64 { return s.CpusNrPeriods },
				labelNames, attrLabels},
			prometheus.NewDesc(
				"mesos_agent_cpu_nr_throttled_total",
				"Total number of throttled CFS enforcement intervals.",
				labelNames, nil,
			): metric{prometheus.CounterValue, func(s *statistics) float64 { return s.CpusNrThrottled },
				labelNames, attrLabels},

			// Memory
			prometheus.NewDesc(
				"mesos_agent_mem_anon_bytes",
				"Current anonymous memory in bytes",
				labelNames, nil,
			): metric{prometheus.GaugeValue, func(s *statistics) float64 { return s.MemAnonBytes },
				labelNames, attrLabels},
			prometheus.NewDesc(
				"mesos_agent_mem_limit_bytes",
				"Current memory limit in bytes",
				labelNames, nil,
			): metric{prometheus.GaugeValue, func(s *statistics) float64 { return s.MemLimitBytes },
				labelNames, attrLabels},
			prometheus.NewDesc(
				"mesos_agent_mem_rss_bytes",
				"Current rss memory usage",
				labelNames, nil,
			): metric{prometheus.GaugeValue, func(s *statistics) float64 { return s.MemRssBytes },
				labelNames, attrLabels},
			prometheus.NewDesc(
				"mesos_agent_mem_total_bytes",
				"Current total memory usage",
				labelNames, nil,
			): metric{prometheus.GaugeValue, func(s *statistics) float64 { return s.MemTotalBytes },
				labelNames, attrLabels},
			prometheus.NewDesc(
				"mesos_agent_mem_cache_bytes",
				"Current page cache memory usage",
				labelNames, nil,
			): metric{prometheus.GaugeValue, func(s *statistics) float64 { return s.MemCacheBytes },
				labelNames, attrLabels},
			prometheus.NewDesc(
				"mesos_agent_mem_swap_bytes",
				"Current swap usage",
				labelNames, nil,
			): metric{prometheus.GaugeValue, func(s *statistics) float64 { return s.MemSwapBytes },
				labelNames, attrLabels},
			prometheus.NewDesc(
				"mesos_agent_mem_file_bytes",
				"Current file bytes count",
				labelNames, nil,
			): metric{prometheus.GaugeValue, func(s *statistics) float64 { return s.MemFileBytes },
				labelNames, attrLabels},
			prometheus.NewDesc(
				"mesos_agent_mem_mapped_file_bytes",
				"Current memory mapped file bytes count",
				labelNames, nil,
			): metric{prometheus.GaugeValue, func(s *statistics) float64 { return s.MemMappedFileBytes },
				labelNames, attrLabels},
			prometheus.NewDesc(
				"mesos_agent_mem_unevictable_bytes",
				"Current memory unevictable bytes count",
				labelNames, nil,
			): metric{prometheus.GaugeValue, func(s *statistics) float64 { return s.MemUnevictableBytes },
				labelNames, attrLabels},
			prometheus.NewDesc(
				"mesos_agent_mem_low_pressure_counter",
				"Low pressure counter value",
				labelNames, nil,
			): metric{prometheus.CounterValue, func(s *statistics) float64 { return s.MemLowPressureCounter },
				labelNames, attrLabels},
			prometheus.NewDesc(
				"mesos_agent_mem_medium_pressure_counter",
				"Medium pressure counter value",
				labelNames, nil,
			): metric{prometheus.CounterValue, func(s *statistics) float64 { return s.MemMediumPressureCounter },
				labelNames, attrLabels},
			prometheus.NewDesc(
				"mesos_agent_critical_low_pressure_counter",
				"Critical pressure counter value",
				labelNames, nil,
			): metric{prometheus.CounterValue, func(s *statistics) float64 { return s.MemCriticalPressureCounter },
				labelNames, attrLabels},

			// Disk
			prometheus.NewDesc(
				"mesos_agent_disk_limit_bytes",
				"Current disk limit in bytes",
				labelNames, nil,
			): metric{prometheus.GaugeValue, func(s *statistics) float64 { return s.DiskLimitBytes },
				labelNames, attrLabels},
			prometheus.NewDesc(
				"mesos_agent_disk_used_bytes",
				"Current disk usage",
				labelNames, nil,
			): metric{prometheus.GaugeValue, func(s *statistics) float64 { return s.DiskUsedBytes },
				labelNames, attrLabels},

			// Network
			// - RX
			prometheus.NewDesc(
				"mesos_agent_network_receive_bytes_total",
				"Total bytes received",
				labelNames, nil,
			): metric{prometheus.CounterValue, func(s *statistics) float64 { return s.NetRxBytes },
				labelNames, attrLabels},
			prometheus.NewDesc(
				"mesos_agent_network_receive_dropped_total",
				"Total packets dropped while receiving",
				labelNames, nil,
			): metric{prometheus.CounterValue, func(s *statistics) float64 { return s.NetRxDropped },
				labelNames, attrLabels},
			prometheus.NewDesc(
				"mesos_agent_network_receive_errors_total",
				"Total errors while receiving",
				labelNames, nil,
			): metric{prometheus.CounterValue, func(s *statistics) float64 { return s.NetRxErrors },
				labelNames, attrLabels},
			prometheus.NewDesc(
				"mesos_agent_network_receive_packets_total",
				"Total packets received",
				labelNames, nil,
			): metric{prometheus.CounterValue, func(s *statistics) float64 { return s.NetRxPackets },
				labelNames, attrLabels},
			// - TX
			prometheus.NewDesc(
				"mesos_agent_network_transmit_bytes_total",
				"Total bytes transmitted",
				labelNames, nil,
			): metric{prometheus.CounterValue, func(s *statistics) float64 { return s.NetTxBytes },
				labelNames, attrLabels},
			prometheus.NewDesc(
				"mesos_agent_network_transmit_dropped_total",
				"Total packets dropped while transmitting",
				labelNames, nil,
			): metric{prometheus.CounterValue, func(s *statistics) float64 { return s.NetTxDropped },
				labelNames, attrLabels},
			prometheus.NewDesc(
				"mesos_agent_network_transmit_errors_total",
				"Total errors while transmitting",
				labelNames, nil,
			): metric{prometheus.CounterValue, func(s *statistics) float64 { return s.NetTxErrors },
				labelNames, attrLabels},
			prometheus.NewDesc(
				"mesos_agent_network_transmit_packets_total",
				"Total packets transmitted",
				labelNames, nil,
			): metric{prometheus.CounterValue, func(s *statistics) float64 { return s.NetTxPackets },
				labelNames, attrLabels},
		},
	}
}

func (c *slaveCollector) Collect(ch chan<- prometheus.Metric) {
	stats := []executor{}
	c.fetchAndDecode("/monitor/statistics", &stats)

	for _, exec := range stats {
		for desc, m := range c.metrics {
			// log.Debugf("%s -> %s", desc, httpClient.hostname)
			m.labelValues["framework_id"] = exec.FrameworkID
			m.labelValues["source"] = exec.Source
			ch <- prometheus.MustNewConstMetric(desc, m.valueType, m.get(exec.Statistics),
				getLabelValuesFromMap(m.labelValues, m.labelNames)...)
		}
	}
}

func (c *slaveCollector) Describe(ch chan<- *prometheus.Desc) {
	for metric := range c.metrics {
		ch <- metric
	}
}
