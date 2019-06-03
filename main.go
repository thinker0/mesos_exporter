package main

import (
	"bytes"
	"compress/gzip"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/common/version"
	log "github.com/sirupsen/logrus"
)

var errorCounter = prometheus.NewCounter(prometheus.CounterOpts{
	Namespace: "mesos",
	Subsystem: "collector",
	Name:      "errors_total",
	Help:      "Total number of internal mesos-collector errors.",
})

func init() {
	// Only log the warning severity or above.
	log.SetLevel(log.ErrorLevel)

	prometheus.MustRegister(errorCounter)
}

func getX509CertPool(pemFiles []string) *x509.CertPool {
	pool := x509.NewCertPool()
	for _, f := range pemFiles {
		content, err := ioutil.ReadFile(f)
		if err != nil {
			log.WithField("error", err).Fatal("x509 certificate pool error")
		}
		ok := pool.AppendCertsFromPEM(content)
		if !ok {
			log.WithField("file", f).Fatal("Error parsing .pem file")
		}
	}
	return pool
}

func getX509ClientCertificates(certFile, keyFile string) []tls.Certificate {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		log.WithFields(log.Fields{
			"certFile": certFile,
			"keyFile":  keyFile,
			"error":    err,
		}).Fatal("Error loading TLS client certificates")
	}
	return []tls.Certificate{cert}
}

func mkHTTPClient(hostname string, url string, timeout time.Duration, auth authInfo, certPool *x509.CertPool, certs []tls.Certificate) *httpClient {
	transport := &http.Transport{
		TLSClientConfig: &tls.Config{
			Certificates:       certs,
			RootCAs:            certPool,
			InsecureSkipVerify: auth.skipSSLVerify,
		},
	}

	// HTTP Redirects are authenticated by Go (>=1.8), when redirecting to an identical domain or a subdomain.
	// -> Hijack redirect authentication, since hostnames rarely follow this logic.
	var redirectFunc func(req *http.Request, via []*http.Request) error
	if auth.username != "" && auth.password != "" {
		// Auth information is only available in the current context -> use lambda function
		redirectFunc = func(req *http.Request, via []*http.Request) error {
			req.SetBasicAuth(auth.username, auth.password)
			return nil
		}
	}

	client := &httpClient{
		http.Client{Timeout: timeout, Transport: transport, CheckRedirect: redirectFunc},
		hostname,
		url,
		auth,
		"",
	}

	if auth.strictMode {
		client.auth.signingKey = parsePrivateKey(client)
	}

	if version.Revision != "" {
		client.userAgent = fmt.Sprintf("mesos_exporter/%s (%s)", version.Version, version.Revision)
	} else {
		client.userAgent = fmt.Sprintf("mesos_exporter/%s", version.Version)
	}

	return client
}

func parsePrivateKey(httpClient *httpClient) []byte {
	if _, err := os.Stat(httpClient.auth.privateKey); os.IsNotExist(err) {
		buffer := bytes.NewBuffer([]byte(httpClient.auth.privateKey))
		var key mesosSecret
		if err := json.NewDecoder(buffer).Decode(&key); err != nil {
			log.WithFields(log.Fields{
				"key":   key,
				"error": err,
			}).Error("Error decoding prviate key")
			errorCounter.Inc()
			return []byte{}
		}
		httpClient.auth.username = key.UID
		httpClient.auth.loginURL = key.LoginEndpoint
		return []byte(key.PrivateKey)
	}
	absPath, _ := filepath.Abs(httpClient.auth.privateKey)
	key, err := ioutil.ReadFile(absPath)
	if err != nil {
		log.WithFields(log.Fields{
			"absPath": absPath,
			"error":   err,
		}).Error("Error reading prviate key")
		errorCounter.Inc()
		return []byte{}
	}
	return key
}

func csvInputToList(input string) []string {
	var entryList []string
	if input == "" {
		return entryList
	}
	sanitizedString := strings.Replace(input, " ", "", -1)
	entryList = strings.Split(sanitizedString, ",")
	return entryList
}

func agentsDiscover(masterURL string, timeout time.Duration, auth authInfo, certPool *x509.CertPool, certs []tls.Certificate) ([]*httpClient, error) {
	log.Infof("discovering slaves... %s", masterURL)

	// This will redirect us to the elected mesos master
	redirectURL := fmt.Sprintf("%s/master/redirect", masterURL)
	rReq, err := http.NewRequest("GET", redirectURL, nil)
	if err != nil {
		panic(err)
	}

	tr := http.Transport{
		DisableKeepAlives: true,
	}
	rresp, err := tr.RoundTrip(rReq)
	if err != nil {
		log.WithField("discover", rReq).Error(err)
		return nil, err
	}
	defer rresp.Body.Close()

	// This will/should return http://master.ip:5050
	masterLoc := rresp.Header.Get("Location")
	if masterLoc == "" {
		log.Warnf("%d response missing Location header", rresp.StatusCode)
		return nil, err
	}

	log.Infof("current elected master at: %s", masterLoc)

	// Starting from 0.23.0, a Mesos Master does not set the scheme in the "Location" header.
	// Use the scheme from the master URL in this case.
	var stateURL string
	if strings.HasPrefix(masterLoc, "http") {
		stateURL = fmt.Sprintf("%s/master/state", masterLoc)
	} else {
		stateURL = fmt.Sprintf("%s:%s/master/state", "http", masterLoc)
	}

	var state state

	log.Infof("current master at: %s", stateURL)

	resp, err := http.Get(stateURL)
	if err != nil {
		log.WithField("discover", stateURL).Error(err)
		return nil, err
	}
	defer resp.Body.Close()

	var reader io.ReadCloser
	switch resp.Header.Get("Content-Encoding") {
	case "gzip":
		if reader, err = gzip.NewReader(resp.Body); err != nil {
			log.WithFields(log.Fields{
				"url":   masterURL,
				"error": err,
			}).Error("Error fetching URL")
			errorCounter.Inc()

		}
		defer reader.Close()
	default:
		reader = resp.Body
	}

	// Find all active slaves
	if err := json.NewDecoder(reader).Decode(&state); err != nil {
		log.Warn(err)
		return nil, err
	}

	var slaveURLs []*httpClient
	for _, slave := range state.Slaves {
		if slave.Active {
			// Extract slave port from pid
			host, port, err := net.SplitHostPort(slave.PID)
			if err != nil {
				port = "5051"
			}
			agentUrl := fmt.Sprintf("http://%s:%s", host, port)
			log.Debugf("Agent: %s %s \t%s", slave.Version, slave.Hostname, agentUrl)
			client := mkHTTPClient(slave.Hostname, agentUrl, timeout, auth, certPool, certs)
			slaveURLs = append(slaveURLs, client)
		}
	}
	sort.Slice(slaveURLs, func(i, j int) bool {
		return slaveURLs[i].hostname < slaveURLs[j].hostname
	})
	for idx, client := range slaveURLs {
		log.Debugf("Agent list: %d\t%s", idx, client.hostname)
	}
	log.Infof("%d slaves discovered", len(slaveURLs))
	return slaveURLs, nil
}

func main() {
	fs := flag.NewFlagSet("mesos-exporter", flag.ExitOnError)
	addr := fs.String("addr", ":9105", "Address to listen on")
	masterURL := fs.String("master", "", "Expose metrics from master running on this URL")
	slaveURL := fs.String("slave", "", "Expose metrics from slave running on this URL")
	discoverURL := fs.String("discover", "", "Expose metrics from master running on this URL")
	instance := fs.Int("instance", 1, "Instance Expose metrics from master running on this URL")
	timeout := fs.Duration("timeout", 10*time.Second, "Master polling timeout")
	exportedTaskLabels := fs.String("exportedTaskLabels", "", "Comma-separated list of task labels to include in the corresponding metric")
	exportedSlaveAttributes := fs.String("exportedSlaveAttributes", "", "Comma-separated list of slave attributes to include in the corresponding metric")
	trustedCerts := fs.String("trustedCerts", "", "Comma-separated list of certificates (.pem files) trusted for requests to Mesos endpoints")
	clientCertFile := fs.String("clientCert", "", "Path to Mesos client TLS certificate (.pem file)")
	clientKeyFile := fs.String("clientKey", "", "Path to Mesos client TLS key file (.pem file)")
	strictMode := fs.Bool("strictMode", false, "Use strict mode authentication")
	username := fs.String("username", "", "Username for authentication")
	password := fs.String("password", "", "Password for authentication")
	loginURL := fs.String("loginURL", "https://leader.mesos/acs/api/v1/auth/login", "URL for strict mode authentication")
	logLevel := fs.String("logLevel", "error", "Log level")
	privateKey := fs.String("privateKey", "", "File path to certificate for strict mode authentication")
	skipSSLVerify := fs.Bool("skipSSLVerify", false, "Skip SSL certificate verification")
	vers := fs.Bool("version", false, "Show version")
	enableMasterState := fs.Bool("enableMasterState", true, "Enable collection from the master's /state endpoint")

	fs.Parse(os.Args[1:])

	if *vers {
		fmt.Println(version.Print("mesos_exporter"))
		os.Exit(0)
	}

	if *masterURL != "" && *slaveURL != "" && *discoverURL != "" {
		log.Fatal("Only -master or -slave or -discover can be given at a time")
	}

	// Getting logging setup with the appropriate log level
	logrusLogLevel, err := log.ParseLevel(*logLevel)
	if err != nil {
		log.WithField("logLevel", *logLevel).Fatal("invalid logging level")
	}
	if logrusLogLevel != log.ErrorLevel {
		log.SetLevel(logrusLogLevel)
		log.WithField("logLevel", *logLevel).Info("Changing log level")
	}

	log.Infoln("Starting mesos_exporter", version.Info())
	log.Infoln("Build context", version.BuildContext())

	prometheus.MustRegister(version.NewCollector("mesos_exporter"))

	auth := authInfo{
		strictMode:    *strictMode,
		skipSSLVerify: *skipSSLVerify,
		loginURL:      *loginURL,
	}

	if *strictMode && *privateKey != "" {
		auth.privateKey = *privateKey
	} else {
		auth.privateKey = os.Getenv("MESOS_EXPORTER_PRIVATE_KEY")
		log.WithField("privateKey", auth.privateKey).Debug("strict mode, no private key, pulling from the environment")
	}

	if *username != "" {
		auth.username = *username
	} else {
		auth.username = os.Getenv("MESOS_EXPORTER_USERNAME")
		log.WithField("username", auth.username).Debug("auth with no username, pulling from the environment")
	}

	if *password != "" {
		auth.password = *password
	} else {
		auth.password = os.Getenv("MESOS_EXPORTER_PASSWORD")
		// NOTE it's already in the environment, so can be easily read anyway
		log.WithField("password", auth.password).Debug("auth with no password, pulling from the environment")
	}

	var certPool *x509.CertPool
	if *trustedCerts != "" {
		certPool = getX509CertPool(csvInputToList(*trustedCerts))
	}

	var certs []tls.Certificate
	if (*clientCertFile != "" && *clientKeyFile == "") ||
		(*clientCertFile == "" && *clientKeyFile != "") {
		log.Fatal("Must supply both clientCert and clientKey to use TLS mutual auth")
	}
	if *clientCertFile != "" && *clientKeyFile != "" {
		certs = getX509ClientCertificates(*clientCertFile, *clientKeyFile)
	}

	slaveAttributeLabels := csvInputToList(*exportedSlaveAttributes)
	slaveTaskLabels := csvInputToList(*exportedTaskLabels)

	switch {
	case *discoverURL != "":
		log.Info("Exporter Mode")

		slaveCollectors := []func(*httpClient) prometheus.Collector{
			func(c *httpClient) prometheus.Collector {
				var s slaveState
				log.WithField("url", "/slave(1)/state").Debug("fetching URL")
				c.fetchAndDecode("/slave(1)/state", &s)
				return newSlaveCollector(c, s.Attributes, slaveTaskLabels, slaveAttributeLabels)
			},
			func(c *httpClient) prometheus.Collector {
				var s slaveState
				log.WithField("url", "/slave(1)/state").Debug("fetching URL")
				c.fetchAndDecode("/slave(1)/state", &s)
				return newSlaveMonitorCollector(c, s.Attributes, slaveTaskLabels, slaveAttributeLabels)
			},
			func(c *httpClient) prometheus.Collector {
				var s slaveState
				log.WithField("url", "/slave(1)/state").Debug("fetching URL")
				c.fetchAndDecode("/slave(1)/state", &s)
				return newSlaveStateCollector(c, s, slaveTaskLabels, slaveAttributeLabels)
			},
		}

		agentUrls, err := agentsDiscover(*discoverURL, *timeout, auth, certPool, certs)
		if err != nil {
			log.Fatal("could not parse master/agents: ", err)
		}
		for _, f := range slaveCollectors {
			c := f(agentUrls[*instance])
			if err := prometheus.Register(c); err != nil {
				log.WithField("error", err).Fatal("Prometheus Register() error")
			}
		}
		log.WithField("address", *addr).Info("Exposing slave metrics")

	case *masterURL != "":
		log.WithField("address", *addr).Info("Exposing master metrics")

		u, err := url.Parse(*masterURL)
		if err != nil {
			log.Fatal(err)
		}
		httpClient := mkHTTPClient(u.Hostname(), *masterURL, *timeout, auth, certPool, certs)
		if err != nil {
			log.Fatal("Unable to get the hostname of this machine")
		}
		if err := prometheus.Register(
			newMasterCollector(httpClient)); err != nil {
			log.WithField("error", err).Fatal("Prometheus Register() error")
		}

		if *enableMasterState {
			if err := prometheus.Register(
				newMasterStateCollector(httpClient, slaveAttributeLabels)); err != nil {
				log.WithField("error", err).Fatal("Prometheus Register() error")
			}
		}

	case *slaveURL != "":
		log.WithField("address", *addr).Info("Exposing slave metrics")
		u, err := url.Parse(*slaveURL)
		if err != nil {
			log.Fatal("Unable to get the hostname of this machine")
		}
		slaveCollectors := []func(*httpClient) prometheus.Collector{
			func(c *httpClient) prometheus.Collector {
				var s slaveState
				log.WithField("url", "/slave(1)/state").Debug("fetching URL")
				c.fetchAndDecode("/slave(1)/state", &s)
				return newSlaveCollector(c, s.Attributes, slaveTaskLabels, slaveAttributeLabels)
			},
			func(c *httpClient) prometheus.Collector {
				var s slaveState
				log.WithField("url", "/slave(1)/state").Debug("fetching URL")
				c.fetchAndDecode("/slave(1)/state", &s)
				return newSlaveMonitorCollector(c, s.Attributes, slaveTaskLabels, slaveAttributeLabels)
			},
			func(c *httpClient) prometheus.Collector {
				var s slaveState
				log.WithField("url", "/slave(1)/state").Debug("fetching URL")
				c.fetchAndDecode("/slave(1)/state", &s)
				return newSlaveStateCollector(c, s, slaveTaskLabels, slaveAttributeLabels)
			},
		}

		for _, f := range slaveCollectors {
			if err := prometheus.Register(f(mkHTTPClient(u.Hostname(), *slaveURL, *timeout, auth, certPool, certs))); err != nil {
				log.WithField("error", err).Fatal("Prometheus Register() error")
			}
		}

	default:
		log.Fatal("Either -master or -slave is required")
	}

	log.Info("Listening and serving ...")

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`<html>
            <head><title>Mesos Exporter</title></head>
            <body>
            <h1>Mesos Exporter</h1>
            <p><a href="/metrics">Metrics</a></p>
            </body>
            </html>`))
	})

	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok"))
	})

	http.Handle("/metrics", promhttp.Handler())
	if err := http.ListenAndServe(*addr, nil); err != nil {
		log.WithField("error", err).Fatal("listen and serve error")
	}
}
