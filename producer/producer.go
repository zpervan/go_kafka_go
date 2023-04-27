package main

import (
	"context"
	"github.com/segmentio/kafka-go"
	"log"
)

const (
	hostAddress = "localhost"
	brokerPort  = "9092"
	topicName   = "TutorialTopic"
)

func main() {
	println("starting producer")

	writer := kafka.Writer{
		Addr:     kafka.TCP(hostAddress + ":" + brokerPort),
		Topic:    topicName,
		Balancer: &kafka.LeastBytes{},
	}

	err := writer.WriteMessages(context.Background(),
		kafka.Message{
			Key:   []byte("Key-A"),
			Value: []byte("Hello World!"),
		},
		kafka.Message{
			Key:   []byte("Key-A"),
			Value: []byte("Well, hello again, World!"),
		},
	)

	if err != nil {
		log.Fatal("failed to write messages:", err)
	}

	if err := writer.Close(); err != nil {
		log.Fatal("failed to close writer:", err)
	}
}
