package main

import (
	"bufio"
	"context"
	"encoding/json"
	"log"
	"os"
	"strings"

	"github.com/segmentio/kafka-go"
)

var producer = &kafka.Writer{
	Addr:      kafka.TCP("localhost:9092"),
	Topic:     "mass-producer",
	BatchSize: 100,
}

type user struct {
	Name  string
	Age   string
	Email string
}

func main() {
	l := log.Default()
	l.Println("Inicio")

	messages := make(chan []byte)

	go readFile("file.csv", messages)

	var bufferedMessages []kafka.Message
	for msg := range messages {
		bufferedMessages = append(bufferedMessages, kafka.Message{
			Value: msg,
		})

		if len(bufferedMessages) == 500 {
			produceMessages(bufferedMessages)
			bufferedMessages = nil
		}
	}

	l.Println("fim")

}

func readFile(path string, messages chan []byte) {
	defer close(messages)

	file, err := os.Open(path)
	if err != nil {
		panic(err)
	}
	reader := bufio.NewScanner(file)

	for reader.Scan() {
		parseLine(reader.Text(), messages)
	}
}

func parseLine(line string, messages chan []byte) {
	parts := strings.Split(line, ",")
	data, _ := json.Marshal(user{
		Name:  parts[0],
		Age:   parts[1],
		Email: parts[2],
	})

	messages <- data
}

func produceMessages(messages []kafka.Message) {
	err := producer.WriteMessages(context.Background(), messages...)
	if err != nil {
		log.Fatal("failed to write messages:", err)
	}
}
