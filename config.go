package main

import "github.com/MitAyush/experiment1/models"

func NewConfig() *models.Config {
	return &models.Config{
		DBUsername:   "experiment",
		DBPassword:   "experiment123",
		DBHost:       "localhost",
		DBPort:       "5432",
		DefaultDB:    "orders_db",
		KafkaBrokers: []string{"localhost:9092"},
		WorkerCnt:    1,
	}
}
