package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

var ErrPoolClosed = errors.New("pool is closed")

type ChannelPool struct {
	channels chan ChannelInterface
	done     chan struct{}
	once     sync.Once
}

// NewChannelPool initialize a new channel pool
func NewChannelPool(maxSize int) *ChannelPool {
	return &ChannelPool{
		channels: make(chan ChannelInterface, maxSize),
		done:     make(chan struct{}),
	}
}

// Get get a new channel from pool with timeout
func (p *ChannelPool) Get(ctx context.Context) (ChannelInterface, error) {
	select {
	case <-p.done:
		return nil, ErrPoolClosed
	case channel, ok := <-p.channels:
		if !ok {
			return nil, ErrPoolClosed
		}
		return channel, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Put put a channel back to the pool
func (p *ChannelPool) Put(channel ChannelInterface) {
	if channel == nil {
		return
	}

	select {
	case <-p.done:
		errCh := channel.Close()
		if errCh != nil {
			fmt.Println("Error closing channel in pool Put: ", errCh) // colocar otel logger
		}
		return
	default:
	}

	select {
	case p.channels <- channel:
	default:
		errCh := channel.Close()
		if errCh != nil {
			fmt.Println("Error closing channel in second select of pool Put: ", errCh) // colocar otel logger
		}
	}
}

// Close close channel pool and release all channels
func (p *ChannelPool) Close() {
	p.once.Do(func() {
		close(p.done)
		close(p.channels)
		for channel := range p.channels {
			if channel != nil {
				errCh := channel.Close()
				if errCh != nil {
					fmt.Println("Error closing channel in pool: ", errCh) // colocar otel logger
				}
			}
		}
	})
}

// Size returns the current size of the channel pool
func (p *ChannelPool) Size() int {
	return len(p.channels)
}
