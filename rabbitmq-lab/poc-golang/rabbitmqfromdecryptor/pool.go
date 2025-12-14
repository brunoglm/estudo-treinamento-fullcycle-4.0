package rabbitmq

import (
	"context"
	"errors"
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
		channel.Close()
		return
	default:
	}

	select {
	case p.channels <- channel:
	default:
		channel.Close()
	}
}

// Close close channel pool and release all channels
func (p *ChannelPool) Close() {
	p.once.Do(func() {
		close(p.done)
		close(p.channels)
		for channel := range p.channels {
			if channel != nil {
				channel.Close()
			}
		}
	})
}

// Size returns the current size of the channel pool
func (p *ChannelPool) Size() int {
	return len(p.channels)
}
