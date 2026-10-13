package main

type Event struct {
	ID string 
	Name string
	Type string
	Data map[string]interface{}
	Timestamp int64
}

type Producer struct {
	
}