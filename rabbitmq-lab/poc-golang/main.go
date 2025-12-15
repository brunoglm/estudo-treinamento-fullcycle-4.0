package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	rabbitmq "pocgolang/rabbitmqfromdecryptor"
	"syscall"
	"time"
)

func main() {
	amqpURL := "amqp://admin:password123@localhost:5672/"
	fmt.Println(amqpURL)

	os.Setenv("APP_CONN_QUEUE", amqpURL)

	rabbitMQManager := rabbitmq.NewRabbitMQManager()

	err := rabbitMQManager.CreateConnection()
	if err != nil {
		panic(fmt.Sprintf("Failed to create RabbitMQ connection: %v", err))
	}

	err = rabbitMQManager.CreateChannelPool()
	if err != nil {
		panic(fmt.Sprintf("Failed to create RabbitMQ channel pool: %v", err))
	}

	go rabbitMQManager.HandleReconnect()

	publisher := rabbitmq.NewPublisher(rabbitMQManager)

	rabbitmq.SetupInfra(rabbitMQManager)

	go func() {
		for {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

			message := "Hello, RabbitMQ!"
			fmt.Printf("Publishing message: %s\n", message)

			err := publisher.SendMessage(ctx, []byte(message), "amq.direct", "boleto-queue", "test-routing-key")
			if err != nil {
				fmt.Printf("Failed to publish message: %v\n", err) //logar com otel
			} else {
				fmt.Println("Message published:", message)
			}

			time.Sleep(2 * time.Second)
			cancel()
		}
	}()

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, syscall.SIGINT, syscall.SIGTERM)

	<-interrupt
	fmt.Println("Shutting down gracefully...")
	errRabbit := rabbitMQManager.Close()
	if errRabbit != nil {
		fmt.Printf("Context: main - error during RabbitMQManager shutdown: %v\n", errRabbit) //logar com otel
	}
	fmt.Println("Shutdown complete.")
}
