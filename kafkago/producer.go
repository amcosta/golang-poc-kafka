package kafkago

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	kafka "github.com/segmentio/kafka-go"
)

var topic = "segmentio"

func Producer1() {
	topic := "semantio"
	partition := 0
	conn, err := kafka.DialLeader(context.Background(), "tcp", "localhost:9092", topic, partition)
	if err != nil {
		fmt.Errorf("failed to dial leader:", err)
	}

	conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	_, err = conn.WriteMessages(
		kafka.Message{Value: []byte("one")},
		kafka.Message{Value: []byte("two")},
		kafka.Message{Value: []byte("three")},
	)

	if err != nil {
		fmt.Errorf("failed to write messages:", err)
	}

	if err := conn.Close(); err != nil {
		panic(err)
	}

	os.Exit(1)
}

func Producer2() {
	w := &kafka.Writer{
		Addr:  kafka.TCP("localhost:9092"),
		Topic: "semantio",
	}

	err := w.WriteMessages(context.Background(),
		kafka.Message{
			Value: []byte("Primeiro"),
		},
	)

	if err != nil {
		log.Fatal("failed to write messages:", err)
	}

	if err := w.Close(); err != nil {
		log.Fatal("failed to close writer:", err)
	}
}

func Producer3() {
	w := &kafka.Writer{
		Addr:        kafka.TCP("localhost:9092"),
		Topic:       topic,
		Compression: kafka.Gzip,
	}

	err := w.WriteMessages(context.Background(),
		kafka.Message{
			Value: []byte("Primeiro"),
		},
	)

	if err != nil {
		log.Fatal("failed to write messages:", err)
	}

	if err := w.Close(); err != nil {
		log.Fatal("failed to close writer:", err)
	}
}
