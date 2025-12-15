package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const operation = "RabbitMQManager"

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

// declare da infra
// CreateConnection
// CreateChannelPool
// HandleReconnect
// handleConnectionRecovery
// handleChannelRecovery
// connect
// createChannel

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
