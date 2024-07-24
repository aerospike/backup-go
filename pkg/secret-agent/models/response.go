package models

// Response describes response from aerospike secret agent.
type Response struct {
	SecretValue string `json:"SecretValue,omitempty"`
	Error       string `json:"Error,omitempty"`
}
