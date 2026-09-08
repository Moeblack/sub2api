//go:build integration

// Package containertest provides optional host isolation for integration fixtures.
package containertest

import (
	"errors"
	"net"
	"os"
	"strings"

	"github.com/testcontainers/testcontainers-go"
)

// WithBindAddress keeps test databases off public host interfaces when
// SUB2API_TEST_BIND_ADDRESS=127.0.0.1. Leaving it unset preserves support for
// remote Docker and development containers that connect through a bridge IP.
func WithBindAddress(ports ...string) testcontainers.CustomizeRequestOption {
	return func(req *testcontainers.GenericContainerRequest) error {
		address := strings.TrimSpace(os.Getenv("SUB2API_TEST_BIND_ADDRESS"))
		if address == "" {
			return nil
		}
		if net.ParseIP(address) == nil {
			return errors.New("invalid SUB2API_TEST_BIND_ADDRESS: expected an IP address")
		}
		req.ExposedPorts = make([]string, len(ports))
		for i, port := range ports {
			req.ExposedPorts[i] = net.JoinHostPort(address, "") + ":" + port
		}
		return nil
	}
}
