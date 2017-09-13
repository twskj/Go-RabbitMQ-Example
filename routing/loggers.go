package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"strconv"
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

	allLogLevel := []string{"INFO", "WARN", "ERROR", "FATAL"}
	logLevel := 0
	if len(os.Args) != 2 {
		fmt.Println("Log level undefined")
		logLevel = rnd.Int() % len(allLogLevel)
		fmt.Println("Random to", allLogLevel[logLevel])
	} else {
		num, err := strconv.Atoi(os.Args[1])
		if err != nil {
			fmt.Println("Invalid Log level:", os.Args[1])
			logLevel = rnd.Int() % len(allLogLevel)
			fmt.Println("Random to", allLogLevel[logLevel])
		} else {
			if num >= len(allLogLevel) {
				num = len(allLogLevel) - 1
			}
			logLevel = num

			fmt.Println("Log level", allLogLevel[logLevel])
		}
	}

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

	err = ch.ExchangeDeclare(
		"routing", // name
		"direct",  // type
		true,      // durable
		false,     // auto-deleted
		false,     // internal
		false,     // no-wait
		nil,       // arguments
	)
	failOnError(err, "Failed to declare an exchange")

	q, err := ch.QueueDeclare(
		"",    // name -- empty name = random name --> temp queue
		true,  // durable
		false, // delete when usused
		true,  // exclusive --> auto close after disconnect
		false, // no-wait
		nil,   // arguments
	)
	failOnError(err, "Failed to declare a queue")

	// binding refers to a connection between Exchange and Queue
	err = ch.QueueBind(
		q.Name, // queue name
		allLogLevel[logLevel]+":", // routing key <-- no routing key
		"routing",                 // exchange
		false,
		nil,
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
		for d := range msgs {
			log.Print(string(d.Body))
		}
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever
}
