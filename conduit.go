// Package conduit provides abstractions over computation chains
// similar to classic coroutines and inspired mostly by Haskell's
// conduit library.
// Its main abstractions are chains consisting of a data producer,
// a data consumer and, potentially, a pipe of conduits transforming
// or filtering data on their way down the processing chain.
// Each component of a chain, producer, consumer and each conduit,
// is running in its own goroutine receiving and forwarding
// data through a channel. Chains are therefore not only a way
// for separating of concerns in code design, but also a way to speed up
// processing exploiting multicore architectures.
// 
// The position of a component in the chain determines
// the input data that it can expect and the output data it may have
// to send. Since the data passed on in the chain are modelled
// as interface{}, any data qualifies from the compiler perspective.
// A receiver (conduit or consumer) needs to perform a type assertion
// to safely convert the incoming data to the expected type.
// This may fail. Application developers, therefore, need to pay 
// special attention to data types used in concrete implementations.
package conduit

import (
	"errors"
	"fmt"
	"sync"
)

// Source is an input channel
type Source <-chan interface{}

// Target is an output channel
type Target chan<- interface{}

// Producer creates data and sends them
// down the processing chain.
type Producer interface {
	Produce(trg Target) error
}

// Consumer is the endpoint of the processing chain.
type Consumer interface {
	Consume(src Source) error
}

// Conduit sits in the middle of a processing chain
// receiving data, processing them in some form and sending
// them further down.
type Conduit interface {
	Conduct(src Source, trg Target) error
}

// Chain encapsulates the chain processing
// and hides anything irrelevant for users
// building applications.
// Errors that lead to the termination of one
// or more components can be inspected through Errs.
type Chain struct {
	door  sync.Mutex
	sz    uint32 // buffer size
	e     bool   // there were errors
	p     Producer
	c     Consumer
	pipe  []Conduit
	Errs  []error
}

// Resets the chain for a new round of processing.
func (ch *Chain) reset() {
	ch.Errs = nil
	ch.e = false
}

// Adds an error to the processing chain.
func (ch *Chain) addErr(err error) {

	ch.door.Lock()
	defer ch.door.Unlock()

	if !ch.e {
		ch.e = true
	}
	ch.Errs = append(ch.Errs, err)
}

// Runs one conduit
func (ch *Chain) pipe2pipe(src Source, trg Target, p Conduit) {
	defer close(trg)
	err := p.Conduct(src, trg)
	if err != nil {
		ch.addErr(err)
	}
}

// Starts all conduits
func (ch *Chain) runPipe(c0 chan interface{}) (ret chan interface{}, err error) {
	ret = c0
	src := c0

	for _, p := range ch.pipe {
		trg := make(chan interface{}, ch.sz)
		if trg == nil {
			s := fmt.Sprintf("cannot create channel\n")
			err = errors.New(s)
			break
		}
		go ch.pipe2pipe(src, trg, p)
		src, ret = trg, trg
	}
	return
}

// Run starts the chain.
// If the process was interrupted,
// Run terminates with an error.
// Errors that were reported by faulty components
// are written to Errs and can be inspected afterwards.
func (ch *Chain) Run() error {

	ch.reset()

	c1 := make(chan interface{}, ch.sz)
	if c1 == nil {
		s := fmt.Sprintf("cannot create channel\n")
		return errors.New(s)
	}

	c2, err := ch.runPipe(c1)
	if err != nil {
		s := fmt.Sprintf("cannot run pipe: %v\n", err)
		return errors.New(s)
	}

	go func() {
		defer close(c1)
		perr := ch.p.Produce(c1)
		if perr != nil {
			ch.addErr(perr)
		}
	}()

	cerr := ch.c.Consume(c2)
	if cerr != nil {
		ch.addErr(cerr)
	}
	if (ch.e) {
		return errors.New("Errors occurred")
	}
	return nil
}

// NewChain creates a new chain.
// The method expects a producer and a consumer (both mandatory),
// a pipe of Conduits (which may be nil) and a parameter indicating
// the buffer size of channels.
// Note that the order of conduits in the pipe
// determines the order in which they are chained together and processed.
func NewChain(p Producer, pipe []Conduit, c Consumer, sz uint32) (ch *Chain) {
	if p == nil || c == nil {
		return nil
	}
	ch = new(Chain)
	if ch != nil {
		ch.p = p
		ch.c = c
		ch.pipe = pipe
		ch.sz = sz
		ch.e = false
		ch.Errs = nil
	}
	return
}
