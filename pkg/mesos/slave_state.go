// Scrape the /Slave(1)/State endpoint to Get information on the tasks running
// on executors. Information scraped at this point:
//
// * Labels of running tasks ("mesos_slave_task_labels" series)
// * Attributes of mesos slaves ("mesos_slave_attributes")
package mesos

import (
	"encoding/json"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
)

type (
	SlaveState struct {
		Hostname   string
		Attributes map[string]json.RawMessage `json:"attributes"`
		Frameworks []SlaveFramework           `json:"frameworks"`
	}
	SlaveFramework struct {
		ID        string               `json:"ID"`
		Executors []SlaveStateExecutor `json:"executors"`
	}
	SlaveStateExecutor struct {
		ID     string `json:"id"`
		Name   string `json:"name"`
		Source string `json:"source"`
		Tasks  []Task `json:"tasks"`
	}

	SlaveStateCollector struct {
		*HttpClient
		metrics map[*prometheus.Desc]SlaveMetric
	}
	SlaveMetric struct {
		valueType prometheus.ValueType
		value     func(*SlaveState) []MetricValue
	}
	MetricValue struct {
		result float64
		labels []string
	}
)

func NewSlaveStateCollector(httpClient *HttpClient, s SlaveState, userTaskLabelList []string, slaveAttributeLabelList []string) *SlaveStateCollector {
	c := SlaveStateCollector{httpClient, make(map[*prometheus.Desc]SlaveMetric)}

	defaultTaskLabels := []string{"source", "framework_id", "task_name"}
	normalisedUserTaskLabelList := normaliseLabelList(userTaskLabelList)
	taskLabelList := append(defaultTaskLabels, normalisedUserTaskLabelList...)
	normalisedAttributeLabels := normaliseLabelList(slaveAttributeLabelList)
	taskLabelList = append(defaultTaskLabels, normalisedAttributeLabels...)

	c.metrics[prometheus.NewDesc(
		prometheus.BuildFQName("mesos", "Slave", "task_labels"),
		"Labels assigned to tasks running on slaves",
		taskLabelList,
		nil)] = SlaveMetric{prometheus.CounterValue,
		func(st *SlaveState) []MetricValue {
			res := []MetricValue{}
			for _, f := range st.Frameworks {
				for _, e := range f.Executors {
					for _, t := range e.Tasks {
						//Default labels
						taskLabels := prometheus.Labels{
							"source":       e.Source,
							"framework_id": f.ID,
							"task_name":    t.Name,
						}

						// User labels
						for _, label := range normalisedUserTaskLabelList {
							taskLabels[label] = ""
						}
						for _, label := range t.Labels {
							normalisedLabel := normaliseLabel(label.Key)
							// Ignore labels not explicitly whitelisted by user
							if stringInSlice(normalisedLabel, normalisedUserTaskLabelList) {
								taskLabels[normalisedLabel] = label.Value
							}
						}

						for _, label := range normalisedAttributeLabels {
							taskLabels[label] = ""
						}
						for key, value := range st.Attributes {
							normalisedLabel := normaliseLabel(key)
							if stringInSlice(normalisedLabel, normalisedAttributeLabels) {
								if attribute, err := attributeString(value); err == nil {
									taskLabels[normalisedLabel] = attribute
								}
							}
						}

						res = append(res, MetricValue{1, getLabelValuesFromMap(taskLabels, taskLabelList)})
					}
				}
			}
			return res
		},
	}

	if len(slaveAttributeLabelList) > 0 {
		normalisedAttributeLabels := normaliseLabelList(slaveAttributeLabelList)

		c.metrics[prometheus.NewDesc(
			prometheus.BuildFQName("mesos", "Slave", "attributes"),
			"Attributes assigned to slaves",
			normalisedAttributeLabels,
			nil)] = SlaveMetric{prometheus.CounterValue,
			func(st *SlaveState) []MetricValue {
				slaveAttributes := prometheus.Labels{}

				for _, label := range normalisedAttributeLabels {
					slaveAttributes[label] = ""
				}
				for key, value := range st.Attributes {
					normalisedLabel := normaliseLabel(key)
					if stringInSlice(normalisedLabel, normalisedAttributeLabels) {
						if attribute, err := attributeString(value); err == nil {
							slaveAttributes[normalisedLabel] = attribute
						}
					}
				}

				return []MetricValue{{1, getLabelValuesFromMap(slaveAttributes, normalisedAttributeLabels)}}
			},
		}
	}
	return &c
}

func (c *SlaveStateCollector) Collect(ch chan<- prometheus.Metric) {
	var s SlaveState
	log.WithField("Url", "/Slave(1)/State").Debug("fetching URL")
	c.FetchAndDecode("/Slave(1)/State", &s)
	for d, cm := range c.metrics {
		for _, m := range cm.value(&s) {
			// log.Debugf("%s -> %s", d, m.labels)
			ch <- prometheus.MustNewConstMetric(d, cm.valueType, m.result, m.labels...)
		}
	}
}

func (c *SlaveStateCollector) Describe(ch chan<- *prometheus.Desc) {
	for d := range c.metrics {
		ch <- d
	}
}
