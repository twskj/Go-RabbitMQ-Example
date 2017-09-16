package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
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

var rndsrc = rand.NewSource(time.Now().UnixNano())
var rnd = rand.New(rndsrc)

func randomString(l int) string {
	bytes := make([]byte, l)
	for i := 0; i < l; i++ {
		bytes[i] = byte(randInt(65, 90))
	}
	return string(bytes)
}

func randInt(min int, max int) int {
	return min + rnd.Intn(max-min)
}

func main() {

	buff, err := ioutil.ReadFile("conn.txt")
	if err != nil {
		fmt.Println("Use local RabbitMQ")
		buff = []byte("amqp://guest:guest@localhost:5672/")
	}

	connStr := string(buff)

	conn := connect(connStr)
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	callbackQueue, err := ch.QueueDeclare(
		"",    // name
		false, // durable
		false, // delete when unused
		true,  // exclusive
		false, // no-wait
		nil,   // arguments
	)
	failOnError(err, "Failed to declare a queue")

	resultsQueue, err := ch.Consume(
		callbackQueue.Name, // queue
		"",                 // consumer
		true,               // auto-ack
		false,              // exclusive
		false,              // no-local
		false,              // no-wait
		nil,                // args
	)

	failOnError(err, "Fail to register callback consumer")

	enter := make(chan string)
	sigs := make(chan os.Signal, 1)
	done := make(chan bool)

	go func() {

		allFunctions := []string{"fac", "sum", "mul"}
		n := len(allFunctions)
		ticker := time.NewTicker(time.Millisecond * 1000)
		for _ = range ticker.C {
			tmp := rnd.Int() % n
			val := ""
			switch allFunctions[n] {
			case "sleep":
				val = strconv.Itoa(rnd.Intn(5000))
			case "sum", "mul":
				val = strconv.Itoa(rnd.Intn(5000)) + " " + strconv.Itoa(rnd.Intn(5000))
				enter <- fmt.Sprintf("%v(%v)", allFunctions[tmp], val)
			}
		}
	}()

	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {

		for {
			select {
			case line := <-enter:
				body := []byte(line)
				err = ch.Publish(
					"",    // exchange
					"rpc", // routing key
					false, // mandatory
					false, // immediate
					amqp.Publishing{
						ContentType:   "text/plain",
						CorrelationId: randomString(32),
						ReplyTo:       callbackQueue.Name,
						Body:          body,
					},
				)
				log.Printf(" [x] Call: %s", body)
				failOnError(err, "Failed to publish a message")

			case _ = <-sigs:
				done <- true
			}
		}
	}()

	go func() {
		for result := range resultsQueue {
			log.Printf("Job %v = %v", result.CorrelationId, result.Body)
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
