package rabbitmqinicial

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Configurações
const (
	PoolSize      = 10               // Número de canais abertos simultaneamente (ajuste conforme carga)
	MaxRetries    = 5                // Tentativas de reconexão
	ReconnectWait = 10 * time.Second // Tempo entre tentativas

	// Configurações da Topologia
	ExchangeName = "amq.topic"  // Exchange padrão do RabbitMQ
	QueueName    = "test.queue" // Fila que queremos garantir que existe
	RoutingKey   = "test.queue" // A chave de rota (binding)
)

// --- Estrutura do Cliente ---
type RabbitClient struct {
	url            string
	conn           *amqp.Connection
	channelPool    chan *amqp.Channel // O "Buffer" de canais para performance
	connNotify     chan *amqp.Error   // Canal que avisa se a conexão caiu
	mu             sync.Mutex         // PROTEÇÃO: Impede panic em concorrência
	notifyShutdown chan error         // Canal para notificar shutdown completo
}

// NewRabbitClient inicializa a conexão e o pool
func NewPublisherClient(url string, notifyShutdown chan error) (*RabbitClient, error) {
	client := &RabbitClient{
		url:            url,
		channelPool:    make(chan *amqp.Channel, PoolSize),
		notifyShutdown: notifyShutdown,
	}

	if err := client.connect(); err != nil {
		return nil, err
	}

	// Inicia o monitor de reconexão em background
	go client.reconnectLoop()

	return client, nil
}

// Publish: Método seguro e concorrente para envio de mensagens
func (c *RabbitClient) Publish(ctx context.Context, exchange, routingKey string, payload []byte) error {
	// 2. Obtém Canal do Pool (Backpressure aqui se o pool estiver cheio)
	var ch *amqp.Channel
	select {
	case ch = <-c.channelPool:
		// Sucesso
	case <-ctx.Done():
		return fmt.Errorf("timeout aguardando canal: %w", ctx.Err())
	}

	// 3. Garante devolução do canal ao pool
	defer func() {
		c.channelPool <- ch
	}()

	// 4. Publicação Síncrona (Confirm)
	confirmation, err := ch.PublishWithDeferredConfirmWithContext(
		ctx,
		exchange,
		routingKey,
		true,  // Mandatory: erro se não houver fila bindada
		false, // Immediate
		amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent, // Grava no disco do broker
			Body:         payload,
			Timestamp:    time.Now(),
		},
	)
	if err != nil {
		return err
	}

	// 5. Espera o ACK do RabbitMQ
	ok, err := confirmation.WaitContext(ctx)
	if err != nil {
		return fmt.Errorf("erro esperando ack: %w", err)
	}
	if !ok {
		return errors.New("mensagem rejeitada (NACK)")
	}

	return nil
}

// connect cria conexão e preenche o pool
func (c *RabbitClient) connect() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// 1. Verificação de Estado (Evita reconexão desnecessária)
	if c.isConnected() {
		log.Println("Já conectado ao RabbitMQ, pulando reconexão.")
		return nil
	}

	log.Println("Conectando ao RabbitMQ...")

	var err error
	c.conn, err = amqp.Dial(c.url)
	if err != nil {
		return fmt.Errorf("falha ao conectar: %w", err)
	}

	// 2. SETUP DA TOPOLOGIA (AQUI ESTÁ A MÁGICA)
	// Se falhar aqui, a gente aborta tudo e tenta de novo no próximo ciclo
	if err := c.setupTopology(); err != nil {
		c.conn.Close()
		return fmt.Errorf("falha na topologia: %w", err)
	}

	c.connNotify = c.conn.NotifyClose(make(chan *amqp.Error, 1))

	// Preencher o Pool de Canais
	// Esvazia pool antigo se houver (caso de reconexão)
drainLoop:
	for {
		select {
		case <-c.channelPool: // Jogando fora canal morto
		default:
			break drainLoop // Pool vazio, podemos parar de drenar
		}
	}

	// Cria novos canais e coloca no pool
	for i := range PoolSize {
		ch, err := c.conn.Channel()
		if err != nil {
			c.conn.Close()
			return fmt.Errorf("falha ao criar canal no pool: %w", err)
		}

		// ATENÇÃO: Habilita Publisher Confirms para confiabilidade
		if err := ch.Confirm(false); err != nil {
			c.conn.Close()
			return fmt.Errorf("falha ao habilitar confirms: %w", err)
		}

		c.channelPool <- ch
		log.Printf("Canal %d criado e adicionado ao pool.", i+1)
	}

	log.Printf("Conectado! Pool de canais criado com tamanho: %d", PoolSize)
	return nil
}

// reconnectLoop monitora a conexão
func (c *RabbitClient) reconnectLoop() {
	for {
		// Bloqueia aqui até a conexão cair ou ser fechada
		err := <-c.connNotify

		// CASO 1: Fechamento Gracioso (Manual)
		if err == nil {
			log.Println("Conexão fechada limpa (Shutdown). Parando reconnect.")
			return
		}

		// CASO 2: Falha Real (Rede/Broker)
		log.Printf("Conexão perdida: %v. Iniciando recuperação...", err)

		// Lógica de Retries com Backoff
		reconnected := false
		for i := range MaxRetries {
			// Tenta reconectar (reutiliza lógica segura de connect)
			if connectErr := c.connect(); connectErr == nil {
				log.Println("Recuperado com sucesso!")
				reconnected = true
				break
			} else {
				log.Printf("... tentativa %d/%d falhou: %v", i+1, MaxRetries, connectErr)
			}

			// wait before next retry
			time.Sleep(ReconnectWait)
		}

		if !reconnected {
			log.Printf("ERRO CRÍTICO: Impossível reconectar após %d tentativas. Abortando.", MaxRetries)
			c.notifyShutdown <- fmt.Errorf("impossível reconectar após %d tentativas", MaxRetries)
			return
		}
	}
}

// setupTopology: Cria filas e bindings necessários antes de começar a operar
// Isso garante que não vamos publicar no vazio.
func (c *RabbitClient) setupTopology() error {
	log.Println("Verificando topologia (Filas e Bindings)...")

	// Cria um canal temporário apenas para configuração
	ch, err := c.conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close() // Fecha este canal assim que terminar a config

	// 1. Declarar a Fila (Idempotente)
	_, err = ch.QueueDeclare(
		QueueName, // nome: "test.queue"
		true,      // durable: Sim (salva no disco)
		false,     // autoDelete: Não (mantém fila mesmo sem consumers)
		false,     // exclusive: Não
		false,     // noWait
		nil,       // args
	)
	if err != nil {
		return fmt.Errorf("falha ao declarar fila: %w", err)
	}

	// 2. Fazer o Bind (Ligar Exchange -> Fila)
	err = ch.QueueBind(
		QueueName,    // nome da fila
		RoutingKey,   // routing key (ex: "test.queue" ou "logs.*")
		ExchangeName, // exchange (ex: "amq.topic")
		false,
		nil,
	)
	if err != nil {
		return fmt.Errorf("falha ao fazer bind: %w", err)
	}

	return nil
}

// Close: Encerramento gracioso
func (c *RabbitClient) Close() error {
	log.Println("Finalizando RabbitClient...")

	if c.isConnected() {
		err := c.conn.Close() // Isso envia nil para connNotify
		if err != nil {
			log.Printf("Erro ao fechar conexão: %v", err)
		}
	}

	log.Println("RabbitClient finalizado.")

	return nil
}

func (c *RabbitClient) isConnected() bool {
	return c.conn != nil && !c.conn.IsClosed()
}
