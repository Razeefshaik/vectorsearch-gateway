// Package config centralizes the small amount of environment-variable
// lookup logic shared by every binary in this module (gatewayd, consumer),
// so each main.go stays a plain wiring list instead of repeating fallback
// rules for addresses and ports.
package config

import "os"

// Getenv returns the environment variable named by key, or fallback if it
// is unset or empty.
func Getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// AddrOrLocalPort resolves a downstream address. It prefers addrKey
// (e.g. EMBED_SEARCH_ADDR) so Docker Compose can point it at a service name
// such as "embed-search:50051". If addrKey is unset -- the normal case for
// local, non-containerized development, where only the port is known --
// it falls back to "localhost:<portKey>".
func AddrOrLocalPort(addrKey, portKey string) string {
	if v := os.Getenv(addrKey); v != "" {
		return v
	}
	return "localhost:" + os.Getenv(portKey)
}
