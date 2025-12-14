package rabbitmq

import (
	"context"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/mock"
)

type RabbitMQManagerMock struct {
	mock.Mock
	pool       *ChannelPoolMock
	connection *amqp.Connection
}

func (r *RabbitMQManagerMock) CreateConnection() error {
	args := r.Called()
	return args.Error(0)
}

func (r *RabbitMQManagerMock) CreateChannelPool() error {
	args := r.Called()
	return args.Error(0)
}

func (r *RabbitMQManagerMock) HandleReconnect() {
	r.Called()
}

func (r *RabbitMQManagerMock) Close() {
	r.Called()
}

func (r *RabbitMQManagerMock) IsHealthy() bool {
	args := r.Called()
	return args.Bool(0)
}

func (r *RabbitMQManagerMock) GetChannel() (ChannelInterface, error) {
	args := r.Called()

	if args.Get(0) == nil {
		return nil, args.Error(1)
	}

	return args.Get(0).(ChannelInterface), args.Error(1)
}

func (r *RabbitMQManagerMock) ReturnChannelToPool(channel ChannelInterface) {
	r.Called(channel)
}

type ChannelPoolMock struct {
	mock.Mock
}

func (c *ChannelPoolMock) Put(ch ChannelInterface) {
	c.Called(ch)
}

func (c *ChannelPoolMock) Get(ctx context.Context) (ChannelInterface, error) {
	args := c.Called(ctx)

	if args.Get(0) == nil {
		return nil, args.Error(1)
	}

	return args.Get(0).(ChannelInterface), args.Error(1)
}

func (c *ChannelPoolMock) Size() int {
	args := c.Called()

	return args.Int(0)
}

func (c *ChannelPoolMock) Close() {
	c.Called()
}

type ChannelMock struct {
	mock.Mock
	confirmationChan chan amqp.Confirmation
}

func (c *ChannelMock) Close() error {
	args := c.Called()
	return args.Error(0)
}

func (c *ChannelMock) Confirm(noWait bool) error {
	args := c.Called(noWait)
	return args.Error(0)
}

func (c *ChannelMock) NotifyClose(ch chan *amqp.Error) chan *amqp.Error {
	args := c.Called(ch)
	return args.Get(0).(chan *amqp.Error)
}

func (c *ChannelMock) NotifyPublish(confirm chan amqp.Confirmation) chan amqp.Confirmation {
	c.Called(confirm)
	return confirm
}

func (c *ChannelMock) Publish(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error {
	args := c.Called(exchange, key, mandatory, immediate, msg)
	return args.Error(0)
}

func (m *ChannelMock) ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, argsTable amqp.Table) error {
	args := m.Called(name, kind, durable, autoDelete, internal, noWait, argsTable)
	return args.Error(0)
}

func (m *ChannelMock) QueueBind(name, key, exchange string, noWait bool, argsTable amqp.Table) error {
	args := m.Called(name, key, exchange, noWait, argsTable)
	return args.Error(0)
}

func (m *ChannelMock) QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, argsTable amqp.Table) (amqp.Queue, error) {
	args := m.Called(name, durable, autoDelete, exclusive, noWait, argsTable)
	return args.Get(0).(amqp.Queue), args.Error(1)
}

func (m *ChannelMock) GetNotifyChannel() chan amqp.Confirmation {
	args := m.Called()
	return args.Get(0).(chan amqp.Confirmation)
}
