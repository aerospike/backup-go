package models

// Request describes request to aerospike secret agent.
type Request struct {
	Resource  string `json:"Resource"`
	SecretKey string `json:"SecretKey"`
}
