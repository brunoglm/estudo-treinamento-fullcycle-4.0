package rabbitmq

import (
	"context"

	amqp "github.com/rabbitmq/amqp091-go"
)

type PublisherInterface interface {
	SendMessage(ctx context.Context, eventData []byte, exchangeName, queueName, routingKey string) error
}

type ChannelInterface interface {
	Close() error
	Confirm(noWait bool) error
	NotifyClose(c chan *amqp.Error) chan *amqp.Error
	NotifyPublish(confirm chan amqp.Confirmation) chan amqp.Confirmation
	Publish(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error
	ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error
	QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error)
	QueueBind(name, key, exchange string, noWait bool, args amqp.Table) error
	GetNotifyChannel() chan amqp.Confirmation
}

// RabbitMQManagerInterface defines the interface for RabbitMQ manager
type RabbitMQManagerInterface interface {
	CreateConnection() error
	CreateChannelPool() error
	HandleReconnect()
	Close()
	IsHealthy() bool
	GetChannel() (ChannelInterface, error)
	ReturnChannelToPool(channel ChannelInterface)
}

type ChannelPoolInterface interface {
	Close()
	Get(ctx context.Context) (ChannelInterface, error)
	Put(ch ChannelInterface)
	Size() int
}
