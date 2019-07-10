package batch_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSendingOnClosedChannel(t *testing.T) {
	ch := make(chan int)
	close(ch)
	assert.Panics(t, func() { ch <- 0 })

	ch = make(chan int, 10)
	close(ch)
	assert.Panics(t, func() { ch <- 0 })
}
