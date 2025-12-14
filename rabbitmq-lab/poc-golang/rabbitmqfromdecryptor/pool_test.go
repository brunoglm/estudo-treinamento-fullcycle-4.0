package rabbitmq

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewChannelPool(t *testing.T) {
	tests := []struct {
		name    string
		maxSize int
	}{
		{
			name:    "should create pool with valid size",
			maxSize: 5,
		},
		{
			name:    "should create pool with zero size",
			maxSize: 0,
		},
		{
			name:    "should create pool with large size",
			maxSize: 100,
		},
	}

	for _, tt := range tests {
		pool := NewChannelPool(tt.maxSize)

		assert.NotNil(t, pool)
		assert.NotNil(t, pool.channels)
		assert.NotNil(t, pool.done)
		assert.Equal(t, 0, pool.Size())
	}
}

func TestChannelPoolPutAndGet(t *testing.T) {
	pool := NewChannelPool(2)
	mockCh := &ChannelMock{}

	pool.Put(mockCh)
	assert.Equal(t, 1, pool.Size())

	ctx := context.Background()
	ch, err := pool.Get(ctx)
	assert.NoError(t, err)
	assert.Equal(t, mockCh, ch)
	assert.Equal(t, 0, pool.Size())
}

func TestChannelPoolGetEmptyPoolWithTimeout(t *testing.T) {
	pool := NewChannelPool(1)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	ch, err := pool.Get(ctx)
	assert.Error(t, err)
	assert.Nil(t, ch)
	assert.Contains(t, err.Error(), "context deadline exceeded")
	assert.Equal(t, 0, pool.Size())
}

func TestChannelPoolPutClosed(t *testing.T) {
	pool := NewChannelPool(1)
	pool.Close()
	mockCh := &ChannelMock{}

	mockCh.On("Close").Return(nil)

	pool.Put(mockCh)
	assert.Equal(t, 0, pool.Size())
	mockCh.AssertCalled(t, "Close")
}

func TestChannelPoolPutFull(t *testing.T) {
	pool := NewChannelPool(1)
	mockCh := &ChannelMock{}
	mockCh2 := &ChannelMock{}

	mockCh2.On("Close").Return(nil)

	pool.Put(mockCh)
	pool.Put(mockCh2)

	assert.Equal(t, 1, pool.Size())
	mockCh.AssertNotCalled(t, "Close")
	mockCh2.AssertCalled(t, "Close")
}

func TestChannelPoolSize(t *testing.T) {
	pool := NewChannelPool(3)

	assert.Equal(t, 0, pool.Size())

	chMock := &ChannelMock{}
	pool.Put(chMock)
	assert.Equal(t, 1, pool.Size())

	pool.Put(chMock)
	assert.Equal(t, 2, pool.Size())

	ctx := context.Background()
	ch, err := pool.Get(ctx)
	assert.NoError(t, err)
	assert.Equal(t, chMock, ch)
	assert.Equal(t, 1, pool.Size())
}

func TestChannelPoolClose(t *testing.T) {
	pool := NewChannelPool(2)
	mockCh1 := &ChannelMock{}
	mockCh2 := &ChannelMock{}

	mockCh1.On("Close").Return(nil)
	mockCh2.On("Close").Return(nil)

	pool.Put(mockCh1)
	pool.Put(mockCh2)

	assert.Equal(t, 2, pool.Size())

	pool.Close()

	assert.Equal(t, 0, pool.Size())
	mockCh1.AssertCalled(t, "Close")
	mockCh2.AssertCalled(t, "Close")
}

func TestChannelPoolGetAfterClose(t *testing.T) {
	pool := NewChannelPool(1)
	mockCh := &ChannelMock{}
	mockCh.On("Close").Return(nil)
	pool.Put(mockCh)
	pool.Close()

	ctx := context.Background()
	ch, err := pool.Get(ctx)

	assert.Error(t, err)
	assert.Nil(t, ch)
	assert.Equal(t, ErrPoolClosed, err)
	assert.Equal(t, 0, pool.Size())
	mockCh.AssertCalled(t, "Close")
}
