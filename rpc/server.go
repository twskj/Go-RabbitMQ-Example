package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"strconv"
	"strings"
	"time"

	"github.com/streadway/amqp"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}

func connect(connStr string) *amqp.Connection {

	conn, err := amqp.Dial(connStr)
	failOnError(err, "Failed to connect to RabbitMQ")
	return conn
}

func main() {

	rndSrc := rand.NewSource(time.Now().UnixNano())
	rnd := rand.New(rndSrc)

	buff, err := ioutil.ReadFile("conn.txt")
	if err != nil {
		fmt.Println("Use local RabbitMQ")
		buff = []byte("amqp://guest:guest@localhost:5672/")
	}

	connStr := string(buff)
	conn := connect(connStr)

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"rpc", // name
		true,  // durable
		false, // delete when usused
		false, // exclusive --> auto close after disconnect
		false, // no-wait
		nil,   // arguments
	)
	failOnError(err, "Failed to declare a queue")

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	forever := make(chan bool)

	go func() {
		for msg := range msgs {
			cmd := string(msg.Body)
			tokens := strings.Fields(cmd)

			switch tokens[0] {
			case "fac":
				sendResult(fib(token[1]))
			case "sum":
				sendResult(sum(token[1], token[2]))
			case "mul":
				sendResult(mul(token[1], token[2]))
			}
		}
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever
}

func factorial(num int) int {
	if num <= 1 {
		return 1
	}

	return num * factorial(n-1)
}

func sum(x1, x2 int) int {
	return strconv.Atoi(x1) + strconv.Atoi(x2)
}

func mul(x1, x2 int) int {
	return strconv.Atoi(x1) * strconv.Atoi(x2)
}

func sendResult(result string) {

}
