package main

import (
	"context"
	"fmt"
	"github.com/segmentio/kafka-go"
	"log"
)

const (
	hostAddress = "localhost"
	brokerPort  = "9092"
	topicName   = "TutorialTopic"
)

func main() {
	println("starting consumer")

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{hostAddress + ":" + brokerPort},
		Topic:     topicName,
		Partition: 0,
		MinBytes:  10e3, // 10KB
		MaxBytes:  10e6, // 10MB
	})

	for {
		m, err := reader.ReadMessage(context.Background())
		if err != nil {
			println("error. reason: " + err.Error())
			break
		}
		fmt.Printf("message at offset %d: %s = %s\n", m.Offset, string(m.Key), string(m.Value))
	}

	if err := reader.Close(); err != nil {
		log.Fatal("failed to close reader:", err)
	}
}
