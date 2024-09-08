package models

type Encryption struct {
	Mode      string
	KeyFile   string
	KeyEnv    string
	KeySecret string
}
