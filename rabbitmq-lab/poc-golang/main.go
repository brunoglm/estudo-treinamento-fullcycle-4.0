package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"pocgolang/rabbitmq"
	"syscall"
	"time"
)

func main() {
	amqpURL := "amqp://admin:password123@localhost:5672/"

	notifyShutdown := make(chan error)

	publisherClient, err := rabbitmq.NewPublisherClient(amqpURL, notifyShutdown)
	if err != nil {
		log.Fatalf("Erro fatal na inicialização: %v", err)
	}

	// Escuta sinais do SO (SIGINT/SIGTERM)
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go publicarMensagem(publisherClient, notifyShutdown)

	log.Println("🚀 Aplicação rodando. Pressione CTRL+C para parar.")

	select {
	case signal := <-sigChan:
		log.Printf("🛑 Sinal de término recebido (%v). Iniciando shutdown...", signal)
	case err := <-notifyShutdown:
		log.Printf("🛑 Notificação de shutdown recebida: %v. Iniciando shutdown...", err)
	case <-time.After(300 * time.Second):
		log.Println("⏰ Tempo limite atingido. Iniciando shutdown...")
	}

	log.Println("✅ Shutdown solicitado. Encerrando conexões...")
	if err := publisherClient.Close(); err != nil {
		log.Printf("Erro ao fechar o cliente RabbitMQ: %v", err)
	} else {
		log.Println("Cliente RabbitMQ fechado com sucesso.")
	}

	log.Println("👋 Aplicação encerrada.")
}

func publicarMensagem(publisherClient *rabbitmq.RabbitClient, notifyShutdown chan error) {
	pubCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := publisherClient.Publish(pubCtx, "amq.topic", "test.queue", []byte("Hello, RabbitMQ! This is a test message 1."))
	if err != nil {
		log.Printf("Erro ao publicar mensagem 1: %v", err)
		notifyShutdown <- err
		return
	}

	err = publisherClient.Publish(pubCtx, "amq.topic", "test.queue", []byte("Hello, RabbitMQ! This is a test message 2."))
	if err != nil {
		log.Printf("Erro ao publicar mensagem 2: %v", err)
		notifyShutdown <- err
		return
	}

	err = publisherClient.Publish(pubCtx, "amq.topic", "test.queue", []byte("Hello, RabbitMQ! This is a test message 3."))
	if err != nil {
		log.Printf("Erro ao publicar mensagem 3: %v", err)
		notifyShutdown <- err
		return
	}
}
