package rabbitmq

import (
	"context"
	"errors"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

const (
	publisherTestExchangeName = "test.exchange"
	publisherTestQueueName    = "test.queue"
	publisherTestRoutingKey   = "test.key"
	publisherTestMessage      = "test message"
	channelErrorMessage       = "channel error"
)

func TestNewPublisher(t *testing.T) {
	mockRabbitMQ := &RabbitMQManagerMock{}
	publisher := NewPublisher(mockRabbitMQ)

	assert.Equal(t, mockRabbitMQ, publisher.RabbitMQManager)
}

func TestPublisherSendMessageGetChannelError(t *testing.T) {
	mockRabbitMQ := &RabbitMQManagerMock{}

	expectedError := errors.New(channelErrorMessage)
	mockRabbitMQ.On("GetChannel").Return(nil, expectedError)

	publisher := NewPublisher(mockRabbitMQ)

	err := publisher.SendMessage(context.TODO(), []byte(publisherTestMessage), publisherTestExchangeName, publisherTestQueueName, publisherTestRoutingKey)

	assert.Error(t, err)
	assert.Equal(t, expectedError, err)
	mockRabbitMQ.AssertExpectations(t)
	mockRabbitMQ.AssertCalled(t, "GetChannel")
	mockRabbitMQ.AssertNotCalled(t, "ReturnChannelToPool")
}

func TestPublisherSendMessageError(t *testing.T) {
	expectedError := errors.New("nack received from the server during message posting")
	confirmationChan := make(chan amqp.Confirmation, 1)

	mockRabbitMQ := &RabbitMQManagerMock{}
	chMock := &ChannelMock{}
	chMock.On("GetNotifyChannel").Return(confirmationChan)
	chMock.On("Publish", publisherTestExchangeName, publisherTestRoutingKey, true, false, mock.Anything).Return(nil)
	chMock.On("Close").Return(nil)
	mockRabbitMQ.On("GetChannel").Return(chMock, nil)
	mockRabbitMQ.On("ReturnChannelToPool", chMock).Return()

	go func() {
		confirmationChan <- amqp.Confirmation{DeliveryTag: 1, Ack: false}
	}()

	publisher := NewPublisher(mockRabbitMQ)

	err := publisher.SendMessage(context.TODO(), []byte(publisherTestMessage), publisherTestExchangeName, publisherTestQueueName, publisherTestRoutingKey)

	assert.Equal(t, expectedError, err)
	chMock.AssertCalled(t, "Close")
	chMock.AssertCalled(t, "GetNotifyChannel")
	chMock.AssertCalled(t, "Publish", publisherTestExchangeName, publisherTestRoutingKey, true, false, mock.Anything)
	mockRabbitMQ.AssertCalled(t, "GetChannel")
	mockRabbitMQ.AssertNotCalled(t, "ReturnChannelToPool")
}
func TestPublisherSendMessageSuccess(t *testing.T) {
	confirmationChan := make(chan amqp.Confirmation, 1)

	mockRabbitMQ := &RabbitMQManagerMock{}
	chMock := &ChannelMock{}
	chMock.On("GetNotifyChannel").Return(confirmationChan)
	chMock.On("Publish", publisherTestExchangeName, publisherTestRoutingKey, true, false, mock.Anything).Return(nil)
	mockRabbitMQ.On("GetChannel").Return(chMock, nil)
	mockRabbitMQ.On("ReturnChannelToPool", chMock).Return()

	go func() {
		confirmationChan <- amqp.Confirmation{DeliveryTag: 1, Ack: true}
	}()

	publisher := NewPublisher(mockRabbitMQ)

	err := publisher.SendMessage(context.TODO(), []byte(publisherTestMessage), publisherTestExchangeName, publisherTestQueueName, publisherTestRoutingKey)

	assert.NoError(t, err)
	chMock.AssertNotCalled(t, "Close")
	chMock.AssertCalled(t, "GetNotifyChannel")
	chMock.AssertCalled(t, "Publish", publisherTestExchangeName, publisherTestRoutingKey, true, false, mock.Anything)
	mockRabbitMQ.AssertCalled(t, "GetChannel")
	mockRabbitMQ.AssertCalled(t, "ReturnChannelToPool", chMock)
}
