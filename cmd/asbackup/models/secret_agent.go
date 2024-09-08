package models

type SecretAgent struct {
	ConnectionType     string
	Address            string
	Port               int
	TimeoutMillisecond int
	CaFile             string
	IsBase64           bool
}
