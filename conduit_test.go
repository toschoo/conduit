package conduit

import (
	"errors"
	"fmt"
	"math/rand"
	"testing"
	"time"
)

const (
	small = 128
	medium = 1024
	big = 8192
)

const (
	numOfTests int = 100
	numOfData  int = 100
	bufSize    int = 5
)

func getSeed() int64 {
	t := time.Now()
	s := t.Second()
	h := t.Hour()
	return int64(3600*h+s)
}

func makeTestData(n int) []int {
	mydata := make([]int, n)
	rand.Seed(getSeed())
	for i:=0; i < n; i++ {
		mydata[i] = rand.Int()
	}
	return mydata
}

type BaseProducer struct {
	src []int
}

func (p *BaseProducer) Produce(trg Target) error {
	for _, v := range p.src {
		trg <- v
	}
	return nil
}

type BaseConsumer struct {
	recvd []int
}

func (c *BaseConsumer) Consume(src Source) error {
	for v := range src {
		i := v.(int)
		c.recvd = append(c.recvd, i)
	}
	return nil
}

type BaseConduit struct {}

func (c *BaseConduit) Conduct(src Source, trg Target) error {
	for v := range src {
		trg <- v
	}
	return nil
}

type BufConduit struct {}

func (c *BufConduit) Conduct(src Source, trg Target) error {
	buf := make([]int, bufSize)
	i := 0
	for {
		if i == bufSize {
			for j:=0; j<i; j++ {
				trg <- buf[j]
			}
			i = 0
		}
		v := <-src
		if v == nil {
			break
		}
		buf[i] = v.(int)
		i++
	}
	if i != 0 {
		for j:=0; j<i; j++ {
			trg <- buf[j]
		}
	}
	return nil
}

var errMsg string = "random error"

type ErrConduit struct{}

func (c *ErrConduit) Conduct(src Source, trg Target) error {
	for v := range src {
		k := rand.Int()%10
		if k == 0 {
			return errors.New(errMsg)
		}
		trg <- v
	}
	return nil
}

// Chain without conduit:
// - It is processed without errors
// - All data are received 
// - in the order in which they were sent
func TestBasicChain(t *testing.T) {
	for i:=0; i<numOfTests; i++ {
		err := testBasicChain(numOfData)
		if err != nil {
			m := fmt.Sprintf("BasicChain failed: %v", err)
			t.Error(m)
		}
	}
}

// Chain with one conduit:
// - It is processed without errors
// - All data are received 
// - in the order in which they were sent
func TestBasicConduitChain(t *testing.T) {
	for i:=0; i<numOfTests; i++ {
		err := testBasicConduitChain(numOfData)
		if err != nil {
			m := fmt.Sprintf("BasicConduitChain failed: %v", err)
			t.Error(m)
		}
	}
}

// Chain with buffering conduit:
// - It is processed without errors
// - All data are received 
// - in the order in which they were sent
func TestBufConduitChain(t *testing.T) {
	for i:=0; i<numOfTests; i++ {
		err := testBufConduitChain(numOfData)
		if err != nil {
			m := fmt.Sprintf("BufConduitChain failed: %v", err)
			t.Error(m)
		}
	}
}

// Chain with n conduits:
// - It is processed without errors
// - All data are received 
// - in the order in which they were sent
func TestNConduitsChain(t *testing.T) {
	for i:=0; i<numOfTests; i++ {
		err := testNConduitsChain(numOfData)
		if err != nil {
			m := fmt.Sprintf("NConduitsChain failed: %v", err)
			t.Error(m)
		}
	}
}

// Chain with one conduit where errors may occur
// - Either an errors occurs,
//   which is correctly reported
// - Or all data are received 
// - in the order in which they were sent
func TestErrConduitChain(t *testing.T) {
	for i:=0; i<numOfTests; i++ {
		err := testErrConduitChain(numOfData)
		if err != nil {
			m := fmt.Sprintf("ErrConduitChain failed: %v", err)
			t.Error(m)
		}
	}
}

// Chain with more data than buffer space and
// the number of data not being a multiple of
// the buffer size:
// - All data are received 
// - in the order in which they were sent
func TestBigDataChain(t *testing.T) {
	for i:=0; i<numOfTests; i++ {
		err := testBigDataChain(medium)
		if err != nil {
			m := fmt.Sprintf("BigDataChain failed: %v", err)
			t.Error(m)
		}
	}
}

func testBasicChain(n int) error {

	mydata := makeTestData(n)

	p := new(BaseProducer)
	p.src = mydata

	c := new(BaseConsumer)

	chn := NewChain(p, nil, c, small)

	err := chn.Run()
	if err != nil {
		m := fmt.Sprintf("error on running chain: %v", err)
		return errors.New(m)
	}
	if len(chn.Errs) > 0 {
		m := fmt.Sprintf("error occurred: %v", chn.Errs)
		return errors.New(m)
	}
	for i:=0; i < n; i++ {
		if mydata[i] != c.recvd[i] {
			return errors.New("Received values differ from original!")
		}
	}
	return nil
}

func testBasicConduitChain(n int) error {

	mydata := makeTestData(n)

	p := new(BaseProducer)
	p.src = mydata

	c := new(BaseConsumer)

	pipe := []Conduit{new(BaseConduit)}

	chn := NewChain(p, pipe, c, small)

	err := chn.Run()
	if err != nil {
		m := fmt.Sprintf("error on running chain: %v", err)
		return errors.New(m)
	}
	if len(chn.Errs) > 0 {
		m := fmt.Sprintf("error occurred: %v", chn.Errs)
		return errors.New(m)
	}
	for i:=0; i < n; i++ {
		if mydata[i] != c.recvd[i] {
			return errors.New("Received values differ from original!")
		}
	}
	return nil
}

func testBufConduitChain(n int) error {

	mydata := makeTestData(n)

	p := new(BaseProducer)
	p.src = mydata

	c := new(BaseConsumer)

	pipe := []Conduit{new(BufConduit)}

	chn := NewChain(p, pipe, c, small)

	err := chn.Run()
	if err != nil {
		m := fmt.Sprintf("error on running chain: %v", err)
		return errors.New(m)
	}
	if len(chn.Errs) > 0 {
		m := fmt.Sprintf("error occurred: %v", chn.Errs)
		return errors.New(m)
	}
	for i:=0; i < n; i++ {
		if mydata[i] != c.recvd[i] {
			return errors.New("Received values differ from original!")
		}
	}
	return nil
}

func testNConduitsChain(n int) error {

	mydata := makeTestData(n)

	nConduits := rand.Int()%10

	p := new(BaseProducer)
	p.src = mydata

	c := new(BaseConsumer)

	pipe := make([]Conduit, nConduits)
	for i:=0; i<nConduits; i++ {

		var con Conduit

		b := rand.Int()%2
		if b == 0 {
			con = new(BaseConduit)
		} else {
			con = new(BufConduit)
		}

		pipe[i] = con
	}

	chn := NewChain(p, pipe, c, small)

	err := chn.Run()
	if err != nil {
		m := fmt.Sprintf("error on running chain: %v", err)
		return errors.New(m)
	}
	if len(chn.Errs) > 0 {
		m := fmt.Sprintf("error occurred: %v", chn.Errs)
		return errors.New(m)
	}
	for i:=0; i < n; i++ {
		if mydata[i] != c.recvd[i] {
			return errors.New("Received values differ from original!")
		}
	}
	return nil
}

func testErrConduitChain(n int) error {

	mydata := makeTestData(n)

	p := new(BaseProducer)
	p.src = mydata

	c := new(BaseConsumer)

	pipe := []Conduit{new(ErrConduit)}

	chn := NewChain(p, pipe, c, small)

	err := chn.Run()
	if err != nil {
		if len(chn.Errs) == 0 {
			m := fmt.Sprintf("unknown error on running chain: %v", err)
			return errors.New(m)
		}
		if len(chn.Errs) != 1 {
			m := fmt.Sprintf("unknown errors in processing: %v", chn.Errs)
			return errors.New(m)
		}
		if chn.Errs[0].Error() != errMsg {
			m := fmt.Sprintf("unknown error in processing: %v", chn.Errs[0])
			return errors.New(m)
		}
		return nil
	}
	if len(chn.Errs) > 0 {
		m := fmt.Sprintf("error occurred: %v", chn.Errs)
		return errors.New(m)
	}
	for i:=0; i < n; i++ {
		if mydata[i] != c.recvd[i] {
			return errors.New("Received values differ from original!")
		}
	}
	return nil
}

func testBigDataChain(n int) error {

	mydata := makeTestData(n)

	p := new(BaseProducer)
	p.src = mydata

	c := new(BaseConsumer)

	chn := NewChain(p, nil, c, 5)

	err := chn.Run()
	if err != nil {
		m := fmt.Sprintf("error on running chain: %v", err)
		return errors.New(m)
	}
	if len(chn.Errs) > 0 {
		m := fmt.Sprintf("error occurred: %v", chn.Errs)
		return errors.New(m)
	}
	for i:=0; i < n; i++ {
		if mydata[i] != c.recvd[i] {
			return errors.New("Received values differ from original!")
		}
	}
	return nil
}

// ------------------------------------------------------------------------
// Benchmarks
// ------------------------------------------------------------------------
type NumProducer struct {
	max int
}

func (p *NumProducer) Produce(trg Target) error {
	for i:=1; i<p.max; i++ {
		trg <- i
	}
	return nil
}

type SumConsumer struct {
	sum int
}

func (c *SumConsumer) Consume(src Source) error {
	for v := range src {
		c.sum += v.(int)
	}
	return nil
}

func BenchmarkSumBasic(b *testing.B) {
	p := new(NumProducer)
	c := new(SumConsumer)
	chn := NewChain(p, nil, c, small)

	for i := 0; i < b.N; i++ {
		p.max = 1000
		c.sum = 0

		err := chn.Run()
		if err != nil {
			fmt.Printf("Error: %v\n", err)
		}
	}
}

func BenchmarkSumBaseline(b *testing.B) {
	sum := 0
	for n := 0; n < b.N; n++ {
		sum = 0
		for i := 0; i < 1000; i++ {
			sum += i
		}
	}
}
