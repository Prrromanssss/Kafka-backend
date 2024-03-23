package main

import (
	"fmt"
	"log"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type OrderPlacer struct {
	producer   *kafka.Producer
	topic      string
	deliveryCh chan kafka.Event
}

func NewOrderProducer(p *kafka.Producer, topic string) *OrderPlacer {
	return &OrderPlacer{
		producer:   p,
		topic:      topic,
		deliveryCh: make(chan kafka.Event, 10000),
	}
}

func (op *OrderPlacer) placeOrder(orderType string, size int) error {
	var (
		format  = fmt.Sprintf("%s - %d", orderType, size)
		payload = []byte(format)
	)

	err := op.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &op.topic,
			Partition: kafka.PartitionAny,
		},
		Value: payload,
	},
		op.deliveryCh,
	)
	if err != nil {
		log.Fatal(err)
	}

	<-op.deliveryCh
	return nil
}

func main() {
	topic := "HVSE"
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"client.id":         "prrromanssss",
		"acks":              "all",
	})
	if err != nil {
		fmt.Printf("Failed to create producer: %s\n", err)
	}

	go func() {
		consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
			"bootstrap.servers": "localhost:9092",
			"group.id":          "foo",
			"auto.offset.reset": "smallest",
		})
		if err != nil {
			log.Fatal(err)
		}
		err = consumer.Subscribe(topic, nil)
		if err != nil {
			log.Fatal(err)
		}
		for {
			ev := consumer.Poll(100)
			switch e := ev.(type) {
			case *kafka.Message:
				fmt.Printf("consumed message from the queue: %s\n", string(e.Value))
			case *kafka.Error:
				fmt.Printf("%v\n", e)
			}
		}
	}()

	op := NewOrderProducer(p, "HVSE")

	for i := 0; i < 1000; i++ {
		if err := op.placeOrder("marker order", i+1); err != nil {
			log.Fatal(err)
		}
		time.Sleep(time.Second * 3)
	}

}
