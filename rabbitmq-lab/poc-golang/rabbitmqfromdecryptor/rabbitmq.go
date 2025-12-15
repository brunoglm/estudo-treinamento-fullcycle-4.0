package rabbitmq

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"os"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type ManagedChannel struct {
	*amqp.Channel
	confirmationChan chan amqp.Confirmation
}

func (mc *ManagedChannel) GetNotifyChannel() chan amqp.Confirmation {
	return mc.confirmationChan
}

type RabbitMQManager struct {
	connection       *amqp.Connection
	pool             ChannelPoolInterface
	notifyConnClose  chan *amqp.Error
	notifyChannClose chan *amqp.Error
	done             chan bool
	poolMaxSize      int
}

// NewRabbitMQManager creates a new RabbitMQManager instance
func NewRabbitMQManager() *RabbitMQManager {
	poolSize := getPoolSize()
	return &RabbitMQManager{
		notifyConnClose:  make(chan *amqp.Error, 1),
		notifyChannClose: make(chan *amqp.Error, poolSize),
		done:             make(chan bool),
		poolMaxSize:      poolSize,
	}
}

func (m *RabbitMQManager) getConnection() *amqp.Connection {
	return m.connection
}

func (m *RabbitMQManager) HandleReconnect() {
	for {
		select {
		case <-m.done:
			return
		case err := <-m.notifyConnClose:
			if err != nil {
				m.handleConnectionRecovery(err)
			}
		case err := <-m.notifyChannClose:
			m.handleChannelRecovery(err)
		}
	}
}

func (m *RabbitMQManager) handleConnectionRecovery(err *amqp.Error) {
	//logar com otel
	// log.Error(errors.New(err.Error()), operation, "Connection closed, attempting to reconnect", nil)

	for {
		m.notifyChannClose = make(chan *amqp.Error, m.poolMaxSize)
		m.notifyConnClose = make(chan *amqp.Error, 1)
		m.connection = nil
		m.pool = nil

		reconnErr := m.connect()
		if reconnErr == nil {
			channelPoolError := m.CreateChannelPool()
			if channelPoolError == nil {
				//logar com otel
				// log.Info(operation, "Connection re-established successfully", nil)
				return
			}
			fmt.Printf("Context: handleConnectionRecovery - error recreating channel pool: %v\n", channelPoolError) //logar com otel
		}

		fmt.Printf("Context: handleConnectionRecovery - error recreating connection: %v\n", reconnErr) //logar com otel
		time.Sleep(getRetryConnectionInterval())
	}
}

func (m *RabbitMQManager) handleChannelRecovery(err *amqp.Error) {
	var chError error

	if err != nil {
		chError = errors.New(err.Error())
	} else {
		chError = errors.New("channel closed without error")
	}

	//logar com otel
	fmt.Println("Context: handleChannelRecovery - channel closed, attempting to fill channel pool: ", chError)
	// log.Error(chError, operation, "Channel closed, attempting to fill channel pool", map[string]string{
	// 	"PoolMaxSize":     strconv.Itoa(m.poolMaxSize),
	// 	"ChannelPoolSize": strconv.Itoa(m.pool.Size()),
	// })

	for m.connection != nil && m.pool != nil && m.pool.Size() < m.poolMaxSize {
		ch, creationErr := m.createChannel()
		if creationErr == nil {
			m.pool.Put(ch)
		}

		fmt.Printf("Context: handleChannelRecovery - error recreating channel: %v\n", creationErr) //logar com otel
		time.Sleep(getRetryChannelInterval())
	}

	//logar com otel
	// log.Info(operation, "Channel pool successfully recreated", map[string]string{
	// 	"PoolMaxSize":     strconv.Itoa(m.poolMaxSize),
	// 	"ChannelPoolSize": strconv.Itoa(m.pool.Size()),
	// })
}

func (m *RabbitMQManager) CreateChannelPool() error {
	pool := NewChannelPool(m.poolMaxSize)

	for i := 0; i < m.poolMaxSize; i++ {
		ch, chErr := m.createChannel()

		if chErr != nil {
			pool.Close()
			return chErr
		}

		pool.Put(ch)
	}

	m.pool = pool

	fmt.Println("Channel pool created with size:", m.poolMaxSize)

	return nil
}

func (m *RabbitMQManager) CreateConnection() error {
	var err error

	for i := 0; i < getMaxConnectionRetries(); i++ {
		err = m.connect()

		if err == nil {
			return nil
		}

		time.Sleep(getRetryConnectionInterval())
	}

	return err
}

func (m *RabbitMQManager) connect() error {
	var err error

	m.connection, err = amqp.DialConfig(os.Getenv("APP_CONN_QUEUE"), amqp.Config{
		Heartbeat: getHeartbeat(),
		TLSClientConfig: &tls.Config{
			MinVersion:         tLSVersion(os.Getenv("APP_QUEUE_MIN_TLS")),
			MaxVersion:         tLSVersion(os.Getenv("APP_QUEUE_MAX_TLS")),
			InsecureSkipVerify: os.Getenv("APP_QUEUE_BYPASS_CERTIFICATE") == "true",
		},
	})

	if err != nil {
		return fmt.Errorf("Context: connect - dialing to rabbitMQ, error: %w", err)
	}

	m.connection.NotifyClose(m.notifyConnClose)

	fmt.Println("Connected to RabbitMQ")

	return nil
}

func (m *RabbitMQManager) createChannel() (ChannelInterface, error) {
	var channel *amqp.Channel
	var err error

	if channel, err = m.connection.Channel(); err != nil {
		fmt.Println("Error creating channel: ", err)
		return nil, fmt.Errorf("Context: createChannel, error: %w", err)
	}

	if err = channel.Confirm(false); err != nil {
		return nil, fmt.Errorf("Context: createChannel - enabling confirm mode, error: %w", err)
	}

	individualNotifyClose := make(chan *amqp.Error, 1)
	channel.NotifyClose(individualNotifyClose)
	confirmationChan := make(chan amqp.Confirmation, 1)
	channel.NotifyPublish(confirmationChan)

	go func() {
		closeErr := <-individualNotifyClose

		select {
		case m.notifyChannClose <- closeErr:
		case <-m.done:
		}
	}()

	return &ManagedChannel{
		channel,
		confirmationChan,
	}, nil
}

func (m *RabbitMQManager) Close() error {
	close(m.done)

	if m.pool != nil {
		m.pool.Close()
	}

	if m.connection != nil {
		err := m.connection.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

// IsHealthy checks if message broker is healthy
func (m *RabbitMQManager) IsHealthy() bool {
	cnx := m.connection

	if cnx == nil {
		return false
	}

	channel, err := cnx.Channel()
	if err != nil {
		return false
	}

	defer func() {
		errCh := channel.Close()
		if errCh != nil {
			fmt.Println("Error closing channel in IsHealthy: ", errCh) // colocar otel logger
		}
	}()

	return true
}

// GetChannel get channel from pool
func (m *RabbitMQManager) GetChannel() (ChannelInterface, error) {
	if m.pool == nil {
		return nil, errors.New("channel pool is not initialized")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	return m.pool.Get(ctx)
}

// ReturnChannelToPool returns channel to pool or closes it if pool is not initialized
func (m *RabbitMQManager) ReturnChannelToPool(channel ChannelInterface) {
	if m.pool != nil && channel != nil {
		m.pool.Put(channel)
	} else if channel != nil {
		errCh := channel.Close()
		if errCh != nil {
			fmt.Println("Error closing channel in ReturnChannelToPool: ", errCh) // colocar otel logger
		}
	}
}

func tLSVersion(version string) uint16 {
	switch version {
	case "1.0":
		return tls.VersionTLS10
	case "1.1":
		return tls.VersionTLS11
	case "1.2":
		return tls.VersionTLS12
	case "1.3":
		return tls.VersionTLS13
	default:
		return tls.VersionTLS12
	}
}

// setup infra

func SetupInfra(rm *RabbitMQManager) error {
	conn := rm.getConnection()
	if conn == nil {
		return errors.New("connection is not established")
	}

	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	defer func() {
		errClose := ch.Close()
		if errClose != nil {
			fmt.Println("Context: SetupInfra - error closing channel: ", errClose) //logar com otel
		}
	}()

	// 1. Declarar a Fila (Idempotente)
	_, err = ch.QueueDeclare(
		"boleto-queue", // nome: "test.queue"
		true,           // durable: Sim (salva no disco)
		false,          // autoDelete: Não (mantém fila mesmo sem consumers)
		false,          // exclusive: Não
		false,          // noWait
		nil,            // args
	)
	if err != nil {
		return fmt.Errorf("falha ao declarar fila: %w", err)
	}

	// 2. Fazer o Bind (Ligar Exchange -> Fila)
	err = ch.QueueBind(
		"boleto-queue",     // nome da fila
		"test-routing-key", // routing key (ex: "test.queue" ou "logs.*")
		"amq.direct",       // exchange (ex: "amq.direct")
		false,
		nil,
	)
	if err != nil {
		return fmt.Errorf("falha ao fazer bind: %w", err)
	}

	return nil
}
