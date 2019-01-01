package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
)

type (
	slave struct {
		Active     bool
		Hostname   string
		PID        string                     `json:"pid"`
		Used       resources                  `json:"used_resources"`
		Unreserved resources                  `json:"unreserved_resources"`
		Total      resources                  `json:"resources"`
		Attributes map[string]json.RawMessage `json:"attributes"`
	}

	framework struct {
		Active    bool   `json:"active"`
		Tasks     []task `json:"tasks"`
		Completed []task `json:"completed_tasks"`
	}

	state struct {
		Slaves     []slave     `json:"slaves"`
		Frameworks []framework `json:"frameworks"`
	}

	masterCollector struct {
		httpClient *httpClient
		metrics     map[prometheus.Collector]func(*state, prometheus.Collector)
	}
)

func newMasterStateCollector(httpClient *httpClient, slaveAttributeLabels []string) prometheus.Collector {
	labels := []string{"slave"}
	metrics := map[prometheus.Collector]func(*state, prometheus.Collector){
		prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Help:      "Total slave CPUs (fractional)",
			Namespace: "mesos",
			Subsystem: "slave",
			Name:      "cpus",
		}, labels): func(st *state, c prometheus.Collector) {
			for _, s := range st.Slaves {
				c.(*prometheus.GaugeVec).WithLabelValues(s.PID).Set(s.Total.CPUs)
			}
		},
		prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Help:      "Used slave CPUs (fractional)",
			Namespace: "mesos",
			Subsystem: "slave",
			Name:      "cpus_used",
		}, labels): func(st *state, c prometheus.Collector) {
			for _, s := range st.Slaves {
				c.(*prometheus.GaugeVec).WithLabelValues(s.PID).Set(s.Used.CPUs)
			}
		},
		prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Help:      "Unreserved slave CPUs (fractional)",
			Namespace: "mesos",
			Subsystem: "slave",
			Name:      "cpus_unreserved",
		}, labels): func(st *state, c prometheus.Collector) {
			for _, s := range st.Slaves {
				c.(*prometheus.GaugeVec).WithLabelValues(s.PID).Set(s.Unreserved.CPUs)
			}
		},
		prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Help:      "Total slave memory in bytes",
			Namespace: "mesos",
			Subsystem: "slave",
			Name:      "mem_bytes",
		}, labels): func(st *state, c prometheus.Collector) {
			for _, s := range st.Slaves {
				c.(*prometheus.GaugeVec).WithLabelValues(s.PID).Set(s.Total.Mem * 1024)
			}
		},
		prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Help:      "Used slave memory in bytes",
			Namespace: "mesos",
			Subsystem: "slave",
			Name:      "mem_used_bytes",
		}, labels): func(st *state, c prometheus.Collector) {
			for _, s := range st.Slaves {
				c.(*prometheus.GaugeVec).WithLabelValues(s.PID).Set(s.Used.Mem * 1024)
			}
		},
		prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Help:      "Unreserved slave memory in bytes",
			Namespace: "mesos",
			Subsystem: "slave",
			Name:      "mem_unreserved_bytes",
		}, labels): func(st *state, c prometheus.Collector) {
			for _, s := range st.Slaves {
				c.(*prometheus.GaugeVec).WithLabelValues(s.PID).Set(s.Unreserved.Mem * 1024)
			}
		},
		prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Help:      "Total slave disk space in bytes",
			Namespace: "mesos",
			Subsystem: "slave",
			Name:      "disk_bytes",
		}, labels): func(st *state, c prometheus.Collector) {
			for _, s := range st.Slaves {
				c.(*prometheus.GaugeVec).WithLabelValues(s.PID).Set(s.Total.Disk * 1024)
			}
		},
		prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Help:      "Used slave disk space in bytes",
			Namespace: "mesos",
			Subsystem: "slave",
			Name:      "disk_used_bytes",
		}, labels): func(st *state, c prometheus.Collector) {
			for _, s := range st.Slaves {
				c.(*prometheus.GaugeVec).WithLabelValues(s.PID).Set(s.Used.Disk * 1024)
			}
		},
		prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Help:      "Unreserved slave disk in bytes",
			Namespace: "mesos",
			Subsystem: "slave",
			Name:      "disk_unreserved_bytes",
		}, labels): func(st *state, c prometheus.Collector) {
			for _, s := range st.Slaves {
				c.(*prometheus.GaugeVec).WithLabelValues(s.PID).Set(s.Unreserved.Disk * 1024)
			}
		},
		prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Help:      "Total slave ports",
			Namespace: "mesos",
			Subsystem: "slave",
			Name:      "ports",
		}, labels): func(st *state, c prometheus.Collector) {
			for _, s := range st.Slaves {
				size := s.Total.Ports.size()
				c.(*prometheus.GaugeVec).WithLabelValues(s.PID).Set(float64(size))
			}
		},
		prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Help:      "Used slave ports",
			Namespace: "mesos",
			Subsystem: "slave",
			Name:      "ports_used",
		}, labels): func(st *state, c prometheus.Collector) {
			for _, s := range st.Slaves {
				size := s.Used.Ports.size()
				c.(*prometheus.GaugeVec).WithLabelValues(s.PID).Set(float64(size))
			}
		},
		prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Help:      "Unreserved slave ports",
			Namespace: "mesos",
			Subsystem: "slave",
			Name:      "ports_unreserved",
		}, labels): func(st *state, c prometheus.Collector) {
			for _, s := range st.Slaves {
				size := s.Unreserved.Ports.size()
				c.(*prometheus.GaugeVec).WithLabelValues(s.PID).Set(float64(size))
			}
		},
	}

	if len(slaveAttributeLabels) > 0 {
		normalisedAttributeLabels := normaliseLabelList(slaveAttributeLabels)
		slaveAttributesLabelsExport := append(labels, normalisedAttributeLabels...)

		metrics[counter("slave", "attributes", "Attributes assigned to slaves", slaveAttributesLabelsExport...)] = func(st *state, c prometheus.Collector) {
			for _, s := range st.Slaves {
				slaveAttributesExport := prometheus.Labels{
					"slave": s.PID,
				}

				// User labels
				for _, label := range normalisedAttributeLabels {
					slaveAttributesExport[label] = ""
				}
				for key, value := range s.Attributes {
					normalisedLabel := normaliseLabel(key)
					if stringInSlice(normalisedLabel, normalisedAttributeLabels) {
						if attribute, err := attributeString(value); err == nil {
							slaveAttributesExport[normalisedLabel] = attribute
						}
					}
				}
				c.(*settableCounterVec).Set(1, getLabelValuesFromMap(slaveAttributesExport, slaveAttributesLabelsExport)...)
			}
		}
	}

	return &masterCollector{
		httpClient: httpClient,
		metrics:     metrics,
	}
}

func (c *masterCollector) Collect(ch chan<- prometheus.Metric) {
	var s state
	log.WithField("url", "/state").Debug("fetching URL")
	c.httpClient.fetchAndDecode("/state", &s)
	for c, set := range c.metrics {
		set(&s, c)
		c.Collect(ch)
	}
}

func (c *masterCollector) Describe(ch chan<- *prometheus.Desc) {
	for metric := range c.metrics {
		metric.Describe(ch)
	}
}

type ranges [][2]uint64

func (rs *ranges) UnmarshalJSON(data []byte) (err error) {
	if data = bytes.Trim(data, `[]"`); len(data) == 0 {
		return nil
	}

	var rng [2]uint64
	for _, r := range bytes.Split(data, []byte(",")) {
		ps := bytes.SplitN(r, []byte("-"), 2)
		if len(ps) != 2 {
			return fmt.Errorf("bad range: %s", r)
		}

		rng[0], err = strconv.ParseUint(string(bytes.TrimSpace(ps[0])), 10, 64)
		if err != nil {
			return err
		}

		rng[1], err = strconv.ParseUint(string(bytes.TrimSpace(ps[1])), 10, 64)
		if err != nil {
			return err
		}

		*rs = append(*rs, rng)
	}

	return nil
}

func (rs ranges) size() uint64 {
	var sz uint64
	for i := range rs {
		sz += 1 + (rs[i][1] - rs[i][0])
	}
	return sz
}
