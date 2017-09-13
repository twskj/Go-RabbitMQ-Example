package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"strings"
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

	// notice no Queue declare here
	err = ch.ExchangeDeclare(
		"glob_routing", // name
		"topic",        // type
		true,           // durable
		false,          // auto-deleted
		false,          // internal
		false,          // no-wait
		nil,            // arguments
	)

	failOnError(err, "Failed to declare a queue")

	enter := make(chan string)
	sigs := make(chan os.Signal, 1)
	done := make(chan bool)

	go func() {

		rnd := rand.NewSource(time.Now().UnixNano())
		logType := []string{"INFO", "WARN", "ERROR", "FATAL"}
		logSource := []string{"HTTP", "DB", "FS", "OTHER"}
		n := len(logType)
		srcLen := len(logSource)
		ticker := time.NewTicker(time.Millisecond * 1000)
		for _ = range ticker.C {
			tmp := rnd.Int63()
			enter <- fmt.Sprintf("%v %v: event key %v", logSource[rand.Int()%srcLen], logType[tmp%int64(n)], tmp)
		}
	}()

	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {

		for {
			select {
			case line := <-enter:
				body := []byte(line)
				tokens := strings.Fields(line)
				err = ch.Publish(
					"glob_routing",          // exchange
					tokens[0]+"."+tokens[1], // routing key
					false, // mandatory
					false, // immediate
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
