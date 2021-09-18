package sarama

import (
	"context"
	"fmt"

	"github.com/Shopify/sarama"
)

func Consumer1() {
	consumer := &Consumer{
		ready: make(chan bool),
	}

	config := sarama.NewConfig()
	config.Consumer.Offsets.AutoCommit.Enable = false
	config.Consumer.Fetch.Min = 2

	// config.Version = version

	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	client, err := sarama.NewConsumerGroup(brokerList, "sarama", config)
	if err != nil {
		fmt.Errorf(err.Error())
	}

	if err := client.Consume(context.Background(), []string{"test-sarama"}, consumer); err != nil {
		fmt.Errorf("Error from consumer: %v", err)
	}
}

// Consumer represents a Sarama consumer group consumer
type Consumer struct {
	ready chan bool
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (consumer *Consumer) Setup(sarama.ConsumerGroupSession) error {
	// Mark the consumer as ready
	close(consumer.ready)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (consumer *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (consumer *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	// NOTE:
	// Do not move the code below to a goroutine.
	// The `ConsumeClaim` itself is called within a goroutine, see:
	// https://github.com/Shopify/sarama/blob/main/consumer_group.go#L27-L29
	for message := range claim.Messages() {
		fmt.Printf("%d", claim.InitialOffset())
		// fmt.Printf("Message claimed: value = %s, timestamp = %v, topic = %s", string(message.Value), message.Timestamp, message.Topic)
		fmt.Printf("%+v\n\n", message)
		session.MarkMessage(message, "")
		session.Commit()
	}

	// time.Sleep(3 * time.Second)
	return nil
}
