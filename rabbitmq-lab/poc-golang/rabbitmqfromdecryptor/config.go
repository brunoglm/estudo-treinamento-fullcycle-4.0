package rabbitmq

import (
	"os"
	"strconv"
	"time"
)

func getPoolSize() int {
	if size, err := strconv.Atoi(os.Getenv("APP_RABBITMQ_CHANNEL_POOL_SIZE")); err == nil && size > 0 {
		return size
	}
	return 10
}

func getMaxConnectionRetries() int {
	if retries, err := strconv.Atoi(os.Getenv("APP_RABBITMQ_MAX_CONNECTION_RETRIES")); err == nil && retries > 0 {
		return retries
	}

	return 10
}

func getRetryConnectionInterval() time.Duration {
	if interval, err := strconv.Atoi(os.Getenv("APP_RABBITMQ_RETRY_CONNECTION_INTERVAL")); err == nil && interval > 0 {
		return time.Duration(interval) * time.Millisecond
	}
	return 10000 * time.Millisecond
}

func getRetryChannelInterval() time.Duration {
	if interval, err := strconv.Atoi(os.Getenv("APP_RABBITMQ_RETRY_CHANNEL_INTERVAL")); err == nil && interval > 0 {
		return time.Duration(interval) * time.Millisecond
	}

	return 5000 * time.Millisecond
}

func getAckTimeout() time.Duration {
	if timeout, err := strconv.Atoi(os.Getenv("APP_RABBITMQ_ACK_TIMEOUT")); err == nil && timeout > 0 {
		return time.Duration(timeout) * time.Millisecond
	}

	return 5000 * time.Millisecond
}

func getHeartbeat() time.Duration {
	if heartbeat, err := strconv.Atoi(os.Getenv("APP_MESSAGE_BROKER_HEARTBEAT")); err == nil && heartbeat > 0 {
		return time.Duration(heartbeat) * time.Second
	}

	return time.Duration(10) * time.Second
}
