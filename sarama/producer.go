package sarama

import (
	"encoding/json"

	"github.com/Shopify/sarama"
	"github.com/hashicorp/go-uuid"
)

var (
	brokerList = []string{"localhost:9092"}
)

type Payload1 struct {
	Id         string  `json:"id"`
	ConsumerId int     `json:"consumer_id"`
	Value      float64 `json:"value"`
}

func Producer1() {
	uuid, _ := uuid.GenerateUUID()
	payload := &Payload1{
		Id:         uuid,
		ConsumerId: 1346,
		Value:      124.99,
	}

	content, _ := json.Marshal(payload)
	// msg := &sarama.ProducerMessage{
	// 	Topic: "test-sarama",
	// 	Value: sarama.StringEncoder(string(content)),
	// }

	msg := &sarama.ProducerMessage{
		Topic: "test-sarama",
		Value: sarama.ByteEncoder(content),
	}

	producer, err := sarama.NewSyncProducer(brokerList, nil)
	if err != nil {
		panic(err)
	}

	producer.SendMessage(msg)
}
