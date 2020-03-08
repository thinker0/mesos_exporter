package mesos

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
)

type (
	Slave struct {
		Active         bool
		PID            string                     `json:"pid"`
		Hostname       string                     `json:"hostname"`
		Used           Resources                  `json:"used_resources"`
		Unreserved     Resources                  `json:"unreserved_resources"`
		Total          Resources                  `json:"Resources"`
		RegisteredTime float32                    `json:"registered_time"`
		Version        string                     `json:"version"`
		Attributes     map[string]json.RawMessage `json:"attributes"`
	}

	Framework struct {
		Active    bool   `json:"active"`
		Tasks     []Task `json:"tasks"`
		Completed []Task `json:"completed_tasks"`
	}

	State struct {
		Slaves     []Slave     `json:"slaves"`
		Frameworks []Framework `json:"frameworks"`
	}

	MasterCollector struct {
		*HttpClient
		Metrics map[prometheus.Collector]func(*State, prometheus.Collector)
	}
)

func NewMasterStateCollector(httpClient *HttpClient, slaveAttributeLabels []string) prometheus.Collector {
	labels := []string{"Slave"}
	normalisedAttributeLabels := normaliseLabelList(slaveAttributeLabels)
	labels = append(labels, normalisedAttributeLabels...)
	slaveAttributes := prometheus.Labels{}

	for _, label := range normalisedAttributeLabels {
		slaveAttributes[label] = ""
	}

	metrics := map[prometheus.Collector]func(*State, prometheus.Collector){
		prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Help:      "Total Slave CPUs (fractional)",
			Namespace: "mesos",
			Subsystem: "Slave",
			Name:      "cpus",
		}, labels): func(st *State, c prometheus.Collector) {
			for _, s := range st.Slaves {
				setAttributes(s, normalisedAttributeLabels, slaveAttributes)
				c.(*prometheus.GaugeVec).WithLabelValues(AddValueFromMap(s.PID, slaveAttributes, normalisedAttributeLabels)...).Set(s.Total.CPUs)
			}
		},
		prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Help:      "Used Slave CPUs (fractional)",
			Namespace: "mesos",
			Subsystem: "Slave",
			Name:      "cpus_used",
		}, labels): func(st *State, c prometheus.Collector) {
			for _, s := range st.Slaves {
				setAttributes(s, normalisedAttributeLabels, slaveAttributes)
				c.(*prometheus.GaugeVec).WithLabelValues(AddValueFromMap(s.PID, slaveAttributes, normalisedAttributeLabels)...).Set(s.Used.CPUs)
			}
		},
		prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Help:      "Unreserved Slave CPUs (fractional)",
			Namespace: "mesos",
			Subsystem: "Slave",
			Name:      "cpus_unreserved",
		}, labels): func(st *State, c prometheus.Collector) {
			for _, s := range st.Slaves {
				setAttributes(s, normalisedAttributeLabels, slaveAttributes)
				c.(*prometheus.GaugeVec).WithLabelValues(AddValueFromMap(s.PID, slaveAttributes, normalisedAttributeLabels)...).Set(s.Unreserved.CPUs)
			}
		},
		prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Help:      "Total Slave memory in bytes",
			Namespace: "mesos",
			Subsystem: "Slave",
			Name:      "mem_bytes",
		}, labels): func(st *State, c prometheus.Collector) {
			for _, s := range st.Slaves {
				setAttributes(s, normalisedAttributeLabels, slaveAttributes)
				c.(*prometheus.GaugeVec).WithLabelValues(AddValueFromMap(s.PID, slaveAttributes, normalisedAttributeLabels)...).Set(s.Total.Mem * 1024)
			}
		},
		prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Help:      "Used Slave memory in bytes",
			Namespace: "mesos",
			Subsystem: "Slave",
			Name:      "mem_used_bytes",
		}, labels): func(st *State, c prometheus.Collector) {
			for _, s := range st.Slaves {
				setAttributes(s, normalisedAttributeLabels, slaveAttributes)
				c.(*prometheus.GaugeVec).WithLabelValues(AddValueFromMap(s.PID, slaveAttributes, normalisedAttributeLabels)...).Set(s.Used.Mem * 1024)
			}
		},
		prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Help:      "Unreserved Slave memory in bytes",
			Namespace: "mesos",
			Subsystem: "Slave",
			Name:      "mem_unreserved_bytes",
		}, labels): func(st *State, c prometheus.Collector) {
			for _, s := range st.Slaves {
				setAttributes(s, normalisedAttributeLabels, slaveAttributes)
				c.(*prometheus.GaugeVec).WithLabelValues(AddValueFromMap(s.PID, slaveAttributes, normalisedAttributeLabels)...).Set(s.Unreserved.Mem * 1024)
			}
		},
		prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Help:      "Total Slave disk space in bytes",
			Namespace: "mesos",
			Subsystem: "Slave",
			Name:      "disk_bytes",
		}, labels): func(st *State, c prometheus.Collector) {
			for _, s := range st.Slaves {
				setAttributes(s, normalisedAttributeLabels, slaveAttributes)
				c.(*prometheus.GaugeVec).WithLabelValues(AddValueFromMap(s.PID, slaveAttributes, normalisedAttributeLabels)...).Set(s.Total.Disk * 1024)
			}
		},
		prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Help:      "Used Slave disk space in bytes",
			Namespace: "mesos",
			Subsystem: "Slave",
			Name:      "disk_used_bytes",
		}, labels): func(st *State, c prometheus.Collector) {
			for _, s := range st.Slaves {
				setAttributes(s, normalisedAttributeLabels, slaveAttributes)
				c.(*prometheus.GaugeVec).WithLabelValues(AddValueFromMap(s.PID, slaveAttributes, normalisedAttributeLabels)...).Set(s.Used.Disk * 1024)
			}
		},
		prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Help:      "Unreserved Slave disk in bytes",
			Namespace: "mesos",
			Subsystem: "Slave",
			Name:      "disk_unreserved_bytes",
		}, labels): func(st *State, c prometheus.Collector) {
			for _, s := range st.Slaves {
				setAttributes(s, normalisedAttributeLabels, slaveAttributes)
				c.(*prometheus.GaugeVec).WithLabelValues(AddValueFromMap(s.PID, slaveAttributes, normalisedAttributeLabels)...).Set(s.Unreserved.Disk * 1024)
			}
		},
		prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Help:      "Total Slave ports",
			Namespace: "mesos",
			Subsystem: "Slave",
			Name:      "ports",
		}, labels): func(st *State, c prometheus.Collector) {
			for _, s := range st.Slaves {
				size := s.Total.Ports.size()
				setAttributes(s, normalisedAttributeLabels, slaveAttributes)
				c.(*prometheus.GaugeVec).WithLabelValues(AddValueFromMap(s.PID, slaveAttributes, normalisedAttributeLabels)...).Set(float64(size))
			}
		},
		prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Help:      "Used Slave ports",
			Namespace: "mesos",
			Subsystem: "Slave",
			Name:      "ports_used",
		}, labels): func(st *State, c prometheus.Collector) {
			for _, s := range st.Slaves {
				size := s.Used.Ports.size()
				setAttributes(s, normalisedAttributeLabels, slaveAttributes)
				c.(*prometheus.GaugeVec).WithLabelValues(AddValueFromMap(s.PID, slaveAttributes, normalisedAttributeLabels)...).Set(float64(size))
			}
		},
		prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Help:      "Unreserved Slave ports",
			Namespace: "mesos",
			Subsystem: "Slave",
			Name:      "ports_unreserved",
		}, labels): func(st *State, c prometheus.Collector) {
			for _, s := range st.Slaves {
				size := s.Unreserved.Ports.size()
				setAttributes(s, normalisedAttributeLabels, slaveAttributes)
				c.(*prometheus.GaugeVec).WithLabelValues(AddValueFromMap(s.PID, slaveAttributes, normalisedAttributeLabels)...).Set(float64(size))
			}
		},
	}

	if len(slaveAttributeLabels) > 0 {
		normalisedAttributeLabels := normaliseLabelList(slaveAttributeLabels)
		slaveAttributesLabelsExport := append([]string{"Slave"}, normalisedAttributeLabels...)

		metrics[counter("Slave", "attributes", "Attributes assigned to slaves", slaveAttributesLabelsExport...)] = func(st *State, c prometheus.Collector) {
			for _, s := range st.Slaves {
				slaveAttributesExport := prometheus.Labels{
					"Slave": s.PID,
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
				c.(*SettableCounterVec).Set(1, getLabelValuesFromMap(slaveAttributesExport, slaveAttributesLabelsExport)...)
			}
		}
	}

	return &MasterCollector{
		HttpClient: httpClient,
		Metrics:    metrics,
	}
}

func setAttributes(s Slave, normalisedAttributeLabels []string, slaveAttributes prometheus.Labels) {
	for key, value := range s.Attributes {
		normalisedLabel := normaliseLabel(key)
		if stringInSlice(normalisedLabel, normalisedAttributeLabels) {
			if attribute, err := attributeString(value); err == nil {
				slaveAttributes[normalisedLabel] = attribute
			}
		}
	}
}

func (c *MasterCollector) Collect(ch chan<- prometheus.Metric) {
	var s State
	log.WithField("Url", "/State").Debug("fetching URL")
	c.FetchAndDecode("/State", &s)
	for c, set := range c.Metrics {
		set(&s, c)
		c.Collect(ch)
	}
}

func (c *MasterCollector) Describe(ch chan<- *prometheus.Desc) {
	for metric := range c.Metrics {
		metric.Describe(ch)
	}
}

func (rs *Ranges) UnmarshalJSON(data []byte) (err error) {
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

func (rs Ranges) size() uint64 {
	var sz uint64
	for i := range rs {
		sz += 1 + (rs[i][1] - rs[i][0])
	}
	return sz
}
