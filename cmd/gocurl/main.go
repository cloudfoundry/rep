package main

import (
	"bytes"
	"crypto/tls"
	"errors"
	"flag"
	"log"
	"net/http"
	"os"

	"code.cloudfoundry.org/cfhttp"
)

var (
	tlsCACertPath = flag.String("cacert", "", "CA certificate for API server")
	tlsCertPath   = flag.String("cert", "", "TLS certificate for API server")
	tlsKeyPath    = flag.String("key", "", "TLS private key for API server")
	httpMethod    = flag.String("X", "GET", "HTTP method")
)

func main() {
	flag.Parse()
	if len(flag.Args()) != 1 {
		log.Println("Must provide a URL to contact")
		os.Exit(1)
	}

	var tlsConfig *tls.Config
	var err error
	if *tlsKeyPath != "" || *tlsCertPath != "" || *tlsCACertPath != "" {
		tlsConfig, err = cfhttp.NewTLSConfig(*tlsCertPath, *tlsKeyPath, *tlsCACertPath)
		if err != nil {
			log.Printf("TLS config mismatch: %s\n", err)
			os.Exit(1)
		}
	}
	client := http.Client{
		Transport: &http.Transport{
			TLSClientConfig: tlsConfig,
		},
	}

	url := flag.Arg(0)
	var resp *http.Response
	switch *httpMethod {
	case "GET":
		resp, err = client.Get(url)
	case "POST":
		resp, err = client.Post(url, "", bytes.NewReader([]byte{}))
	default:
		err = errors.New("Currently only supports GET and POST methods")
	}
	if err != nil {
		log.Printf("Failed to contact %s: %s\n", url, err)
		os.Exit(1)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		log.Printf("Error talking to %s: status code %d\n", url, resp.StatusCode)
		os.Exit(1)
	}
	os.Exit(0)
}
