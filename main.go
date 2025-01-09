package main

import (
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/go-faker/faker/v4"
)

var topic = "name_stream"
var kafkaConfig = kafka.ConfigMap{
	"bootstrap.servers": "localhost:19092",
	"acks":              "all",
}

type NameEvent struct {
	FirstName string `json:"first_name"`
	LastName  string `json:"last_name"`
}

func genName() NameEvent {
	return NameEvent{
		FirstName: faker.FirstName(),
		LastName:  faker.LastName(),
	}
}

func streamName(queue chan<- NameEvent, quit <-chan os.Signal) {
	for {
		select {
		case <-quit:
			close(queue)
			return
		default:
			queue <- genName()
		}
	}
}

func sendName(queue <-chan NameEvent) {
	p, err := kafka.NewProducer(&kafkaConfig)
	if err != nil {
		log.Fatalln("failed to create producer with error: ", err)
	}

	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error == nil {
					log.Printf("successfully produced message %s into %s[%d]@%s\n",
						string(ev.Value), *ev.TopicPartition.Topic, ev.TopicPartition.Partition, ev.TopicPartition.Offset.String())
				} else {
					log.Println("failed to produced with error: ", ev.TopicPartition.Error)
				}
			}
		}
	}()

	for e := range queue {
		b, err := json.Marshal(e)
		if err != nil {
			log.Fatalln("failed marshal event with error: ", err)
		}

		p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          b,
		}, nil)
	}

	p.Flush(15 * 1000)
	p.Close()
}

func main() {
	queue := make(chan NameEvent, 50000)
	quit := make(chan os.Signal, 1)

	wg := sync.WaitGroup{}
	wg.Add(2)
	go func() {
		streamName(queue, quit)
		wg.Done()
	}()

	go func() {
		sendName(queue)
		wg.Done()
	}()

	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)
	wg.Wait()
}
