package main

import (
	"fmt"
	"github.com/segmentio/kafka-go"
	"context"
	"time"
)

func runConsumer(){
	topic := "messaging-api"
	partition := 0

	conn, _ := kafka.DialLeader(context.Background(), "tcp", "localhost:9092", topic, partition)

	conn.SetReadDeadline(time.Now().Add(10*time.Second))
	batch := conn.ReadBatch(10e3, 1e6) // fetch 10KB min, 1MB max

	b := make([]byte, 10e3) // 10KB max per message
	for {
	    _, err := batch.Read(b)
	    if err != nil {
	        break
	    }
	    fmt.Println(string(b))
	}

	batch.Close()
	conn.Close()
}

func main() {
	runConsumer()
}


// client = ApiCore::Kafka::Client.new(topic: 'messaging-api').deliver( event: 'invite:friend', payload: { data: { consumer: 'Hello Darina'} } )
// client.deliver( event: 'invite:friend', payload: { data: { consumer: 'Hello Darina'} } }
