package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"strconv"
	"strings"

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

	// rndSrc := rand.NewSource(time.Now().UnixNano())
	// rnd := rand.New(rndSrc)

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
			log.Printf("Received: %v", cmd)
			tokens := strings.Fields(cmd)

			switch tokens[0] {
			case "fac":
				val, err := strconv.Atoi(tokens[1])
				if err != nil {
					sendResult(ch, "ERROR", msg.ReplyTo, msg.CorrelationId)
				} else {
					sendResult(ch, strconv.Itoa(factorial(val)), msg.ReplyTo, msg.CorrelationId)
				}
			case "sum":
				val1, err := strconv.Atoi(tokens[1])
				if err != nil {
					sendResult(ch, "ERROR", msg.ReplyTo, msg.CorrelationId)
					continue
				}
				val2, err := strconv.Atoi(tokens[2])
				if err != nil {
					sendResult(ch, "ERROR", msg.ReplyTo, msg.CorrelationId)
					continue
				}
				sendResult(ch, strconv.Itoa(sum(val1, val2)), msg.ReplyTo, msg.CorrelationId)
			case "mul":
				val1, err := strconv.Atoi(tokens[1])
				if err != nil {
					sendResult(ch, "ERROR", msg.ReplyTo, msg.CorrelationId)
					continue
				}
				val2, err := strconv.Atoi(tokens[2])
				if err != nil {
					sendResult(ch, "ERROR", msg.ReplyTo, msg.CorrelationId)
					continue
				}
				sendResult(ch, strconv.Itoa(mul(val1, val2)), msg.ReplyTo, msg.CorrelationId)
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

	return num * factorial(num-1)
}

func sum(x1, x2 int) int {
	return x1 + x2
}

func mul(x1, x2 int) int {
	return x1 * x2
}

func sendResult(ch *amqp.Channel, result, replyTo, ID string) {
	body := []byte(result)
	err := ch.Publish(
		"",      // exchange
		replyTo, // routing key
		false,   // mandatory
		false,   // immediate
		amqp.Publishing{
			ContentType:   "text/plain",
			CorrelationId: ID,
			Body:          body,
		},
	)
	log.Printf(" [x] Call: %s", body)
	failOnError(err, "Failed to publish a message")
}
