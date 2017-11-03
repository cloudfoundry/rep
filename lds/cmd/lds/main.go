package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"
)

type LDSConfig struct {
	Port int `json:"port"`
}

var port = flag.Int(
	"port",
	-1,
	"Port to run LDS server on",
)

var listenerConfig = flag.String(
	"listener-config",
	"",
	"Path to the listener config",
)

func main() {
	flag.Parse()

	_, err := os.Stat(*listenerConfig)
	if err != nil {
		fmt.Fprintf(os.Stderr, "could not find container proxy listener config at '%s'", *listenerConfig)
		os.Exit(1)
	}
	serverAddr := fmt.Sprintf(":%d", *port)

	s := &http.Server{
		Addr: serverAddr,
		Handler: http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			http.ServeFile(rw, req, *listenerConfig)
		}),
	}
	err = s.ListenAndServe()
	if err != nil {

		fmt.Fprintf(os.Stderr, "container proxy listener discovery service failed to come up: %s", err)
		os.Exit(1)
	}
}
