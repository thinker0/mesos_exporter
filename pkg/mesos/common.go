package mesos

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/dgrijalva/jwt-go"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
)

const LogErrNotFoundInMap = "Couldn't find key in map"

type (
	Ranges [][2]uint64

	Label struct {
		Key   string `json:"key"`
		Value string `json:"Value"`
	}

	Resources struct {
		CPUs  float64 `json:"cpus"`
		Disk  float64 `json:"disk"`
		Mem   float64 `json:"mem"`
		Ports Ranges  `json:"ports"`
	}

	Task struct {
		Name        string    `json:"name"`
		ID          string    `json:"id"`
		ExecutorID  string    `json:"executor_id"`
		FrameworkID string    `json:"framework_id"`
		SlaveID     string    `json:"slave_id"`
		State       string    `json:"State"`
		Labels      []Label   `json:"labels"`
		Resources   Resources `json:"Resources"`
		Statuses    []Status  `json:"statuses"`
	}

	Status struct {
		State     string  `json:"State"`
		Timestamp float64 `json:"timestamp"`
	}

	TokenResponse struct {
		Token string `json:"Token"`
	}

	TokenRequest struct {
		UID   string `json:"uid"`
		Token string `json:"Token"`
	}

	MesosSecret struct {
		LoginEndpoint string `json:"login_endpoint"`
		PrivateKey    string `json:"private_key"`
		Scheme        string `json:"scheme"`
		UID           string `json:"uid"`
	}

	MetricMap map[string]float64

	SettableCounterVec struct {
		Desc   *prometheus.Desc
		Values []prometheus.Metric
	}

	SettableCounter struct {
		Desc  *prometheus.Desc
		Value prometheus.Metric
	}

	AuthInfo struct {
		Username      string
		Password      string
		LoginURL      string
		Token         string
		TokenExpire   int64
		SigningKey    []byte
		StrictMode    bool
		PrivateKey    string
		SkipSSLVerify bool
	}

	HttpClient struct {
		http.Client
		Hostname  string
		Url       string
		Auth      AuthInfo
		UserAgent string
	}

	MetricCollector struct {
		*HttpClient
		Metrics map[prometheus.Collector]func(MetricMap, prometheus.Collector) error
	}
)

var ErrorCounter = prometheus.NewCounter(prometheus.CounterOpts{
	Namespace: "mesos",
	Subsystem: "collector",
	Name:      "errors_total",
	Help:      "Total number of internal mesos-collector errors.",
})

func (c *SettableCounterVec) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.Desc
}

func (c *SettableCounterVec) Collect(ch chan<- prometheus.Metric) {
	for _, v := range c.Values {
		ch <- v
	}

	c.Values = nil
}

func (c *SettableCounterVec) Set(value float64, labelValues ...string) {
	c.Values = append(c.Values, prometheus.MustNewConstMetric(c.Desc, prometheus.CounterValue, value, labelValues...))
}

func (c *SettableCounter) Describe(ch chan<- *prometheus.Desc) {
	if c.Desc == nil {
		log.WithField("counter", c).Warn("NIL description")
	}
	ch <- c.Desc
}

func (c *SettableCounter) Collect(ch chan<- prometheus.Metric) {
	if c.Value == nil {
		log.WithField("counter", c).Warn("NIL Value")
	}
	ch <- c.Value
}

func (c *SettableCounter) Set(value float64) {
	c.Value = prometheus.MustNewConstMetric(c.Desc, prometheus.CounterValue, value)
}

func NewSettableCounter(subsystem, name, help string, labels ...string) *SettableCounter {
	return &SettableCounter{
		Desc: prometheus.NewDesc(
			prometheus.BuildFQName("mesos", subsystem, name),
			help,
			labels,
			prometheus.Labels{},
		),
	}
}

func gauge(subsystem, name, help string, labels ...string) *prometheus.GaugeVec {
	return prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "mesos",
		Subsystem: subsystem,
		Name:      name,
		Help:      help,
	}, labels)
}

func counter(subsystem, name, help string, labels ...string) *SettableCounterVec {
	desc := prometheus.NewDesc(
		prometheus.BuildFQName("mesos", subsystem, name),
		help,
		labels,
		prometheus.Labels{},
	)

	return &SettableCounterVec{
		Desc:   desc,
		Values: nil,
	}
}

func newMetricCollector(httpClient *HttpClient, metrics map[prometheus.Collector]func(MetricMap, prometheus.Collector) error) prometheus.Collector {
	return &MetricCollector{httpClient, metrics}
}

func signingToken(httpClient *HttpClient) string {
	signKey, err := jwt.ParseRSAPrivateKeyFromPEM(httpClient.Auth.SigningKey)
	if err != nil {
		log.WithField("error", err).Error("Error parsing privateKey")
	}

	expireToken := time.Now().Add(time.Hour * 1).Unix()
	httpClient.Auth.TokenExpire = expireToken

	// Create the Token
	token := jwt.NewWithClaims(jwt.SigningMethodRS256, jwt.MapClaims{
		"uid": httpClient.Auth.Username,
		"exp": expireToken,
	})
	log.WithFields(log.Fields{
		"uid":     httpClient.Auth.Username,
		"expires": expireToken,
	}).Debug("creating Token")
	// Sign and Get the complete encoded Token as a string
	tokenString, err := token.SignedString(signKey)
	if err != nil {
		log.WithField("error", err).Error("Error creating login Token")
		return ""
	}
	return tokenString
}

func authToken(httpClient *HttpClient) string {
	currentTime := time.Now().Unix()
	if currentTime > httpClient.Auth.TokenExpire {
		url := httpClient.Auth.LoginURL
		signingToken := signingToken(httpClient)
		body, err := json.Marshal(&TokenRequest{UID: httpClient.Auth.Username, Token: signingToken})
		if err != nil {
			log.WithField("error", err).Error("Error creating JSON request")
			return ""
		}
		buffer := bytes.NewBuffer(body)
		req, err := http.NewRequest("POST", url, buffer)
		if err != nil {
			log.WithFields(log.Fields{
				"Url":   url,
				"error": err,
			}).Error("Error creating HTTP request")
			return ""
		}
		req.Header.Add("User-Agent", httpClient.UserAgent)
		req.Header.Add("Content-Type", "application/json")
		res, err := httpClient.Do(req)
		if err != nil {
			log.WithFields(log.Fields{
				"Url":   url,
				"error": err,
			}).Error("Error fetching URL")
			ErrorCounter.Inc()
			return ""
		}
		defer res.Body.Close()

		var token TokenResponse
		if err := json.NewDecoder(res.Body).Decode(&token); err != nil {
			log.WithFields(log.Fields{
				"Url":   url,
				"error": err,
			}).Error("Error decoding response body")
			ErrorCounter.Inc()
			return ""
		}

		httpClient.Auth.Token = fmt.Sprintf("Token=%s", token.Token)
	}
	return httpClient.Auth.Token
}

func (httpClient *HttpClient) FetchAndDecode(endpoint string, target interface{}) bool {
	url := strings.TrimSuffix(httpClient.Url, "/") + endpoint
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		log.WithFields(log.Fields{
			"Url":   url,
			"error": err,
		}).Error("Error creating HTTP request")
		return false
	}
	req.Header.Add("Accept-Encoding", "gzip")
	req.Header.Add("User-Agent", httpClient.UserAgent)
	if httpClient.Auth.Username != "" && httpClient.Auth.Password != "" {
		req.SetBasicAuth(httpClient.Auth.Username, httpClient.Auth.Password)
	}
	if httpClient.Auth.StrictMode {
		req.Header.Add("Authorization", authToken(httpClient))
	}
	log.WithField("Url", url).Debug("fetching URL")
	res, err := httpClient.Do(req)
	if err != nil {
		log.WithFields(log.Fields{
			"Url":   url,
			"error": err,
		}).Error("Error fetching URL")
		ErrorCounter.Inc()
		return false
	}
	defer res.Body.Close()

	var reader io.ReadCloser
	switch res.Header.Get("Content-Encoding") {
	case "gzip":
		if reader, err = gzip.NewReader(res.Body); err != nil {
			log.WithFields(log.Fields{
				"Url":   url,
				"error": err,
			}).Error("Error fetching URL")
			ErrorCounter.Inc()
		}
		defer reader.Close()
	default:
		reader = res.Body
	}
	if err := json.NewDecoder(reader).Decode(&target); err != nil {
		log.WithFields(log.Fields{
			"Url":   url,
			"error": err,
		}).Error("Error decoding response body")
		ErrorCounter.Inc()
		return false
	}

	return true
}

func (c *MetricCollector) Collect(ch chan<- prometheus.Metric) {
	var m MetricMap
	log.WithField("Url", "/Metrics/snapshot").Debug("fetching URL")
	c.FetchAndDecode("/Metrics/snapshot", &m)
	for cm, f := range c.Metrics {
		if err := f(m, cm); err != nil {
			ch := make(chan *prometheus.Desc, 1)
			log.WithFields(log.Fields{
				"Metric": <-ch,
				"error":  err,
			}).Error("Error extracting Metric")
			ErrorCounter.Inc()
			continue
		}
		cm.Collect(ch)
	}
}

func (c *MetricCollector) Describe(ch chan<- *prometheus.Desc) {
	for m := range c.Metrics {
		m.Describe(ch)
	}
}

var invalidLabelNameCharRE = regexp.MustCompile("(^[^a-zA-Z_])|([^a-zA-Z0-9_])")

// Sanitize Label names according to https://prometheus.io/docs/concepts/data_model/
func normaliseLabel(label string) string {
	return invalidLabelNameCharRE.ReplaceAllString(label, "_")
}

func normaliseLabelList(labelList []string) []string {
	normalisedLabelList := []string{}
	for _, label := range labelList {
		normalisedLabelList = append(normalisedLabelList, normaliseLabel(label))
	}
	return normalisedLabelList
}

func stringInSlice(string string, slice []string) bool {
	for _, elem := range slice {
		if string == elem {
			return true
		}
	}
	return false
}

func getLabelValuesFromMap(labels prometheus.Labels, orderedLabelKeys []string) []string {
	labelValues := []string{}
	for _, label := range orderedLabelKeys {
		labelValues = append(labelValues, labels[label])
	}
	return labelValues
}

var (
	text             = regexp.MustCompile("^[-*[:word:]/.]*$")
	errDropAttribute = errors.New("value neither scalar nor text")
)

// attributeString converts a text attribute in json.RawMessage to string.
// see http://mesos.apache.org/documentation/latest/attributes-resources/
// for more information.  note that scalar matches text for this purpose.
// attributeString returns string or errDropAttribute.
func attributeString(attribute json.RawMessage) (string, error) {
	if value := strings.Trim(string(attribute), `"`); text.MatchString(value) {
		return value, nil
	}
	return "", errDropAttribute
}

func AddValueFromMap(t string, labels prometheus.Labels, orderedLabelKeys []string) []string {
	var labelValues []string
	if len(t) > 0 {
		labelValues = append(labelValues, t)
	}
	for _, label := range orderedLabelKeys {
		labelValues = append(labelValues, labels[label])
	}
	return labelValues
}

func AddValuesFromMap(t []string, labels prometheus.Labels, orderedLabelKeys []string) []string {
	var labelValues []string
	if len(t) > 0 {
		labelValues = append(labelValues, t...)
	}
	for _, label := range orderedLabelKeys {
		labelValues = append(labelValues, labels[label])
	}
	return labelValues
}
