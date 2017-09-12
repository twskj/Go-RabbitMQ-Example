package main

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/streadway/amqp"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}

func main() {

	connStr := "amqp://guest:guest@localhost:5672/"

	conn := connect(connStr)
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	// notice no Queue declare here
	err = ch.ExchangeDeclare(
		"logs",   // name
		"fanout", // type
		true,     // durable
		false,    // auto-deleted
		false,    // internal
		false,    // no-wait
		nil,      // arguments
	)

	failOnError(err, "Failed to declare a queue")

	enter := make(chan string)
	sigs := make(chan os.Signal, 1)
	done := make(chan bool)

	go func() {

		rnd := rand.NewSource(time.Now().UnixNano())
		logType := []string{"INFO", "WARN", "ERROR", "FATAL"}
		n := len(logType)
		ticker := time.NewTicker(time.Millisecond * 1000)
		for _ = range ticker.C {
			tmp := rnd.Int63()
			enter <- fmt.Sprintf("%v: event key %v", logType[tmp%int64(n)], tmp)
		}
	}()

	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {

		for {
			select {
			case line := <-enter:
				body := []byte(line)
				err = ch.Publish(
					"logs", // exchange
					"",     // routing key <-- routing key is empty aka let exchange handle it
					false,  // mandatory
					false,  // immediate
					amqp.Publishing{
						ContentType: "text/plain",
						Body:        []byte(body),
					},
				)
				log.Printf(" [x] Emit log: %s", body)
				failOnError(err, "Failed to publish a message")

			case _ = <-sigs:
				done <- true
			}
		}
	}()

	<-done
	log.Printf("bye")
}

func connect(connStr string) *amqp.Connection {

	conn, err := amqp.Dial(connStr)
	failOnError(err, "Failed to connect to RabbitMQ")
	return conn
}
