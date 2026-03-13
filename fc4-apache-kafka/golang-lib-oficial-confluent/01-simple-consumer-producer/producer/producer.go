package producer

import (
	"encoding/json"
	"fmt"
	"simple-consumer-producer/models"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

var timeout = 5 * time.Second // Tempo máximo para aguardar a confirmação do Kafka

type Producer struct {
	kafkaProducer *kafka.Producer
}

func NewProducer(bootstrapServers string) (*Producer, error) {
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		// --- Conexão Básica ---
		"bootstrap.servers": bootstrapServers,
		"client.id":         "meu-servico-api",

		// --- Confiabilidade e Durabilidade ---
		// "acks":                                  "all",      // Garante que todas as réplicas confirmaram (segurança máxima)
		// "enable.idempotence":                    true,       // Evita duplicatas em caso de retentativas automáticas
		// "retries":                               2147483647, // Tenta reenviar "para sempre" em erros recuperáveis
		// "max.in.flight.requests.per.connection": 5,          // Mantém a ordem mesmo com múltiplos envios

		// --- Performance e Batching ---
		// "compression.type": "snappy", // Comprime os dados (balanço ideal CPU vs Banda)
		// "linger.ms":        20,       // Espera até 20ms para agrupar mensagens em batch
		// "batch.size":                   65536,    // Envia o batch se atingir 64KB antes do linger.ms
		// "queue.buffering.max.messages": 100000,   // Capacidade da fila interna da lib em Go

		// --- Segurança (Exemplo SASL/SSL) ---
		// "security.protocol": "sasl_ssl",
		// "sasl.mechanisms":  "PLAIN",
		// "sasl.username":    "meu_usuario",
		// "sasl.password":    "minha_senha",
	})
	if err != nil {
		return nil, err
	}
	return &Producer{kafkaProducer: p}, nil
}

func (p *Producer) PublishUser(topic string, user models.UserEvent) error {
	payload, _ := json.Marshal(user)

	// Criamos o canal COM BUFFER de 1.
	// IMPORTANTE: Não use 'defer close(deliveryChan)' aqui.
	// Se houver timeout, a lib ainda tentará escrever nele.
	// Com buffer 1 e sem fechar, a lib escreve, ninguém lê, e o GC limpa depois.
	deliveryChan := make(chan kafka.Event, 1)

	headers := []kafka.Header{
		{Key: "correlation_id", Value: []byte("abc-123")},
		{Key: "content_type", Value: []byte("application/json")},
		{Key: "version", Value: []byte("v1")},
	}

	err := p.kafkaProducer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
			// Partition: 2, // Envia especificamente para a partição 2
		},
		Value:   payload,
		Headers: headers,
		Key:     []byte(fmt.Sprintf("%d", user.ID)),
		// Timestamp:      time.Now().UTC(), //Por padrão, o Kafka define o timestamp como o momento em que a mensagem chega ao broker. No entanto, você pode passar o momento exato em que o evento ocorreu no mundo real.
	}, deliveryChan)

	if err != nil {
		return fmt.Errorf("erro ao enfileirar: %w", err)
	}

	select {
	case e := <-deliveryChan:
		// Quando recebemos o evento, o canal não será mais usado.
		m := e.(*kafka.Message)
		if m.TopicPartition.Error != nil {
			return fmt.Errorf("erro de persistência no broker: %w", m.TopicPartition.Error)
		}
		return nil

	case <-time.After(timeout):
		return fmt.Errorf("timeout atingido: o broker não confirmou em %v", timeout)
	}
}

func (p *Producer) Close() {
	p.kafkaProducer.Flush(5000)
	p.kafkaProducer.Close()
}

func (p *Producer) PublishRaw(topic string, data []byte, reason string) error {
	deliveryChan := make(chan kafka.Event, 1)

	err := p.kafkaProducer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          data,
		Headers:        []kafka.Header{{Key: "error_reason", Value: []byte(reason)}},
	}, deliveryChan)

	if err != nil {
		return fmt.Errorf("erro ao enfileirar: %w", err)
	}

	select {
	case e := <-deliveryChan:
		m := e.(*kafka.Message)
		if m.TopicPartition.Error != nil {
			return fmt.Errorf("erro de persistência no broker: %w", m.TopicPartition.Error)
		}
		return nil

	case <-time.After(timeout):
		return fmt.Errorf("timeout atingido: o broker não confirmou em %v", timeout)
	}
}
