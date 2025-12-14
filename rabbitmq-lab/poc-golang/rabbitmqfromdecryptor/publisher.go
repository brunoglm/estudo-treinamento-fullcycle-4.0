package rabbitmq

import (
	"context"
	"errors"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Publisher is a concrete implementation for PublisherInterface
type Publisher struct {
	RabbitMQManager RabbitMQManagerInterface
}

// NewPublisher instantiate a new Publisher
func NewPublisher(rbmq RabbitMQManagerInterface) *Publisher {
	return &Publisher{
		RabbitMQManager: rbmq,
	}
}

// SendMessage publish a message in the queue using channel pool with self-healing
func (p *Publisher) SendMessage(ctx context.Context, eventData []byte, exchangeName, queueName, routingKey string) error {
	var channel ChannelInterface
	var err error

	channel, err = p.RabbitMQManager.GetChannel()
	if err != nil {
		return err
	}

	err = writeMessage(channel, exchangeName, routingKey, eventData)

	if err != nil {
		channel.Close()
		return err
	}

	p.RabbitMQManager.ReturnChannelToPool(channel)

	return nil
}

func writeMessage(channel ChannelInterface, exchangeName, routingKey string, message []byte) error {
	notifyConfirm := channel.GetNotifyChannel()

	err := channel.Publish(
		exchangeName,
		routingKey, // queue
		true,       // mandatory
		false,      // immediate
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "text/plain, charset=UTF-8",
			Body:         message,
		})

	if err == nil {
		select {
		case confirm := <-notifyConfirm:
			if !confirm.Ack {
				err = errors.New("nack received from the server during message posting")
			}
		case <-time.After(getAckTimeout()): // XXXXXXXXXXX Pegar do config em vez disso
			err = errors.New("timeout while waiting for ack")
		}
	}

	return err
}
