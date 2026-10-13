package models


type Config struct {
	DBUsername   string
	DBPassword   string
	DBHost       string
	DBPort       string
	DefaultDB    string
	KafkaBrokers []string
	WorkerCnt    int
}
