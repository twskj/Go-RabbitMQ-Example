package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

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

	q, err := ch.QueueDeclare(
		"helloQueue", // name
		false,        // durable
		false,        // delete when unused
		false,        // exclusive
		false,        // no-wait
		nil,          // arguments
	)

	failOnError(err, "Failed to declare a queue")

	enter := make(chan string)
	sigs := make(chan os.Signal, 1)
	done := make(chan bool)

	go func() {

		fmt.Println("Hit `Enter` to send a message")
		for {
			reader := bufio.NewReader(os.Stdin)
			str, _ := reader.ReadString('\n')
			enter <- str
		}
	}()

	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {

		for {
			select {
			case line := <-enter:
				body := []byte(line)
				err = ch.Publish(
					"",     // exchange
					q.Name, // routing key
					false,  // mandatory
					false,  // immediate
					amqp.Publishing{
						ContentType: "text/plain",
						Body:        []byte(body),
					},
				)

				log.Printf(" [x] Sent %s", body)
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
