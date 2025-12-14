package rabbitmq

import amqp "github.com/rabbitmq/amqp091-go"

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
