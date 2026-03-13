package consumer

import (
	"encoding/json"
	"fmt"
	"log"
	"simple-consumer-producer/models"
	"simple-consumer-producer/producer"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type Consumer struct {
	kafkaConsumer *kafka.Consumer
	producer      *producer.Producer // Necessário para empurrar para a DLQ
}

func NewConsumer(bootstrapServers, groupID string, producer *producer.Producer) (*Consumer, error) {
	config := &kafka.ConfigMap{
		// --- ENDEREÇAMENTO E IDENTIFICAÇÃO ---
		"bootstrap.servers": bootstrapServers,
		"group.id":          groupID,
		"client.id":         "svc-user-processor-v1", // Identifica esta instância nos logs do broker

		// --- ESTRATÉGIA DE LEITURA ---
		// "auto.offset.reset":  "earliest", // Se o offset expirar ou for novo, lê desde o início (evita perda de dados)
		// "enable.auto.commit": false,      // OBRIGATÓRIO EM PRODUÇÃO: Nós decidimos quando a mensagem foi processada com sucesso
		// "auto.commit.interval.ms": 5000,          // Define o intervalo de 5 segundos para commits automáticos (útil para desenvolvimento, mas cuidado em produção)

		// --- RESILIÊNCIA E HEARTBEATS (LIVELINESS) ---
		// Tempo que o broker espera para considerar o consumer "morto" se a rede oscilar
		// "session.timeout.ms": 45000,
		// Intervalo dos sinais de "estou vivo" enviados em background pela lib
		// "heartbeat.interval.ms": 3000,
		// Tempo MÁXIMO entre dois Poll(). Se sua lógica de negócio demorar mais que isso,
		// o Kafka te expulsa do grupo achando que você travou (causando rebalanceamento infinito).
		// "max.poll.interval.ms": 300000, // 5 minutos (ajuste conforme sua latência de processamento)

		// --- PERFORMANCE E LATÊNCIA (THROUGHPUT) ---
		// "fetch.min.bytes":     1,        // Baixa latência: retorna assim que chegar 1 byte // para rodar worker pool, tem que deixar bem alto, pra pegar lote grande
		// "fetch.max.bytes":     52428800, // Limite de 50MB por busca para não estourar memória // para rodar worker pool, tem que deixar bem alto, pra pegar lote grande
		// "fetch.wait.max.ms":   500,      // Espera no máximo 500ms para acumular dados se o tópico estiver calmo
		// "queued.min.messages": 100000,   // Buffer interno da lib: pré-carrega mensagens para o Go processar rápido

		// --- GERENCIAMENTO DE GRUPO ---
		// Estratégia de como as partições são divididas entre as instâncias do seu app.
		// 'cooperative-sticky' evita pausar o consumo de todas as partições durante um deploy (Incremental Rebalance)
		// "partition.assignment.strategy": "cooperative-sticky",

		// --- SEGURANÇA (EXEMPLO SASL/SSL) ---
		// Em produção, você raramente usará conexão aberta (PLAINTEXT)
		// "security.protocol": "sasl_ssl",
		// "sasl.mechanisms":   "PLAIN",
		// "sasl.username":     "seu_api_key",
		// "sasl.password":     "seu_api_secret",

		// --- LOGS E DEBUG ---
		// "log_level": 2, // 0 a 7 (2 é Error, 7 é Debug intenso)
	}
	c, err := kafka.NewConsumer(config)
	if err != nil {
		return nil, err
	}
	return &Consumer{kafkaConsumer: c, producer: producer}, nil
}

func (c *Consumer) ReadUsers(topic string, handler func(models.UserEvent)) {
	c.kafkaConsumer.SubscribeTopics([]string{topic}, nil)

	for {
		msg, err := c.kafkaConsumer.ReadMessage(time.Second)
		if err == nil {
			var user models.UserEvent
			json.Unmarshal(msg.Value, &user)
			handler(user) // Callback para processar o usuário
		}
	}
}

func (c *Consumer) ReadUsersWithWorkerPool(topic string, workerCount int, handler func(models.UserEvent)) {
	c.kafkaConsumer.SubscribeTopics([]string{topic}, nil)

	// Semáforo: um canal com buffer para limitar quantas goroutines rodam ao mesmo tempo
	semaphore := make(chan struct{}, workerCount)

	fmt.Printf("🚀 Iniciando consumidor com %d workers...\n", workerCount)

	for {
		// O timeout de 1s permite que o loop "respire" e aceite sinais de parada
		msg, err := c.kafkaConsumer.ReadMessage(time.Second)

		if err != nil {
			// Kafka.ErrTimedOut é normal quando o tópico está vazio
			if err.(kafka.Error).Code() == kafka.ErrTimedOut {
				continue
			}
			fmt.Printf("❌ Erro no Kafka: %v\n", err)
			continue
		}

		// Bloqueia aqui se todos os workers estiverem ocupados (Backpressure)
		semaphore <- struct{}{}

		// Dispara o processamento em paralelo
		go func(m *kafka.Message) {
			// Garante que o slot no semáforo seja liberado ao final
			defer func() { <-semaphore }()

			var user models.UserEvent
			if err := json.Unmarshal(m.Value, &user); err != nil {
				fmt.Printf("⚠️ Erro ao decodificar: %v\n", err)
				return
			}

			// Processamento de fato
			handler(user)
		}(msg)
	}
}

func (c *Consumer) ReadUsersWithWorkerPoolAndPushDLQ(topic string, workerCount int, handler func(models.UserEvent) error) {
	c.kafkaConsumer.SubscribeTopics([]string{topic}, nil)
	semaphore := make(chan struct{}, workerCount)
	dlqTopic := topic + "-dlq" // Convenção padrão: nome-do-topico-dlq

	fmt.Printf("🚀 Consumer iniciado. Workers: %d | DLQ: %s\n", workerCount, dlqTopic)

	for {
		msg, err := c.kafkaConsumer.ReadMessage(time.Second)
		if err != nil {
			if err.(kafka.Error).Code() == kafka.ErrTimedOut {
				continue
			}
			fmt.Printf("❌ Erro no Kafka: %v\n", err)
			continue
		}

		semaphore <- struct{}{}

		go func(m *kafka.Message) {
			defer func() { <-semaphore }()

			var user models.UserEvent
			if err := json.Unmarshal(m.Value, &user); err != nil {
				fmt.Printf("⚠️ Erro decodificação: %v. Enviando para DLQ...\n", err)
				c.pushToDLQ(dlqTopic, m.Value, "json_parse_error")
				return
			}

			// Chamada do handler (agora esperamos um erro dele)
			if err := handler(user); err != nil {
				fmt.Printf("❌ Falha processamento (ID: %d): %v. Enviando para DLQ...\n", user.ID, err)
				c.pushToDLQ(dlqTopic, m.Value, err.Error())
			}
		}(msg)
	}
}

// pushToDLQ encapsula a lógica de postar a mensagem original no tópico de erro
func (c *Consumer) pushToDLQ(topic string, payload []byte, reason string) {
	if c.producer == nil {
		fmt.Println("🚨 DLQ não configurada no Consumer. Mensagem perdida!")
		return
	}

	// Criamos uma mensagem simples ou podemos adicionar Headers com o motivo do erro
	err := c.producer.PublishRaw(topic, payload, reason)
	if err != nil {
		fmt.Printf("🚨 Falha crítica ao postar na DLQ: %v\n", err)
	}
}

func (c *Consumer) Close() {
	c.kafkaConsumer.Close()
}

func (c *Consumer) ReprocessSpecificOffset(topic string, partition int32, offset int64, handler func(models.UserEvent)) {
	// 1. Em vez de Subscribe (que é dinâmico), usamos Assign para fixar a partição
	tp := kafka.TopicPartition{
		Topic:     &topic,
		Partition: partition,
		Offset:    kafka.Offset(offset), // Definimos onde queremos começar
	}

	err := c.kafkaConsumer.Assign([]kafka.TopicPartition{tp})
	if err != nil {
		log.Fatalf("Erro ao atribuir partição: %v", err)
	}

	fmt.Printf("🎯 Buscando mensagem no Offset %d da Partição %d...\n", offset, partition)

	// 2. Lemos apenas uma mensagem (ou iniciamos um loop limitado)
	msg, err := c.kafkaConsumer.ReadMessage(10 * time.Second)
	if err != nil {
		fmt.Printf("❌ Mensagem não encontrada ou erro: %v\n", err)
		return
	}

	// 3. Processamos o dado
	var user models.UserEvent
	json.Unmarshal(msg.Value, &user)

	fmt.Printf("🔍 Mensagem encontrada! Conteúdo: %+v\n", user)
	handler(user)

	// Nota: Como o group-id é novo e temporário, nem precisamos dar Commit.
}
