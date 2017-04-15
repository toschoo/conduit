package utils

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/toschoo/conduit"
	"math/rand"
	"testing"
	"time"
	"unicode/utf8"
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

func makeTestBytes(n int) []byte {
	mydata := make([]byte, n)
	rand.Seed(getSeed())
	for i:=0; i < n; i++ {
		mydata[i] = byte(rand.Int()%255)
	}
	return mydata
}

type BaseProducer struct {
	src []int
}

func (p *BaseProducer) Produce(trg conduit.Target) error {
	for _, v := range p.src {
		trg <- v
	}
	return nil
}

type BaseConsumer struct {
	recvd []int
}

func (c *BaseConsumer) Consume(src conduit.Source) error {
	for v := range src {
		i := v.(int)
		c.recvd = append(c.recvd, i)
	}
	return nil
}

type ByteConsumer struct {
	recvd []byte
}

func (c *ByteConsumer) Consume(src conduit.Source) error {
	for v := range src {
		buf := v.([]byte)
		c.recvd = append(c.recvd, buf...)
	}
	return nil
}

// Chain with identity conduit:
// - It is processed without errors
// - All data are received 
// - in the order in which they were sent
func TestIdentityConduitChain(t *testing.T) {
	for i:=0; i<numOfTests; i++ {
		err := testIdentityConduitChain(numOfData)
		if err != nil {
			m := fmt.Sprintf("IdentityConduitChain failed: %v", err)
			t.Error(m)
		}
	}
}

// Generator

// ByteReader
// - Process without errors
// - All data are received
// - in the order in which they were sent
func TestByteReaderChain(t *testing.T) {
	for i:=0; i<numOfTests; i++ {
		err := testByteReaderChain(2*big+numOfData)
		if err != nil {
			m := fmt.Sprintf("ByteReaderChain failed: %v", err)
			t.Error(m)
		}
	}
}

// TextReader
// - Process without errors
// - All data are received
// - in the order in which they were sent
func TestTextReaderChain(t *testing.T) {
	for i:=0; i<10*numOfTests; i++ {
		err := testUtf8ConduitChain()
		if err != nil {
			m := fmt.Sprintf("Utf8ConduitChain failed: %v", err)
			t.Error(m)
		}
	}
}

func testIdentityConduitChain(n int) error {

	mydata := makeTestData(n)

	p := new(BaseProducer)
	p.src = mydata

	c := new(BaseConsumer)

	pipe := []conduit.Conduit{NewIdentity()}

	chn := conduit.NewChain(p, pipe, c, small)

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

func testByteReaderChain(n int) error {

	mybytes := makeTestBytes(n)

	rd := bytes.NewReader(mybytes)

	p := NewReader(rd)
	c := new(ByteConsumer)

	chn := conduit.NewChain(p, nil, c, small)

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
		if mybytes[i] != c.recvd[i] {
			return errors.New("Received values differ from original!")
		}
	}
	return nil
}

type Utf8Producer struct {
	rs   []rune
	step int
	sample int
}

// k shuffle
func permute(src []rune) {
	s := len(src)

	for i:=0;i<s;i++ {
		j := rand.Int()%s
		if j != i {
			src[i], src[j] = src[j], src[i]
		}
	}
}

func (p *Utf8Producer) Produce(trg conduit.Target) error {

	rs0 := []rune{'\u0030','\u0031','\u0032','\u0033','\u0020','\u2318','\u0020', '\u2ec6', '\u0020'}

	str := "爾欠少什麼。道流、是爾目前用底、與祖佛不"
	rs1 := []rune(str)

	var sz int

	if p.sample == 0 {
		p.rs = rs0
		sz = 13
	} else {
		p.rs = rs1
		sz = 60
	}

	permute(p.rs)
	s := len(p.rs)
	src := make([]byte, sz)

	j := 0
	for i:=0; i<s; i++ {
		n := utf8.EncodeRune(src[j:], p.rs[i])
		j+=n
	}

	// fmt.Printf("TESTING: %v\n", src)

	n:=0; k:=0
	for i:=0; i < 10*sz; i+=p.step {

		tmp := make([]byte,p.step)
		for j:=0;j<p.step;j++ {
			if n == sz {
				n=0;
			}
			tmp[j] = src[n]
			n++
		}
		// fmt.Printf("sending: %v\n", tmp)
		k++
		if k == p.step {
			k=0
			// fmt.Printf("\n")
		}
		trg <- tmp
	}
	return nil
}

type Utf8Consumer struct {
	rs []rune
}

func (c *Utf8Consumer) Consume(src conduit.Source) error {
	for inp := range src {
		bs := inp.([]byte)

		s := len(bs)
		for i:= 0; i<s; {
			r, n := utf8.DecodeRune(bs[i:])
			if r == utf8.RuneError {
				m := fmt.Sprintf("invalid rune: %v\n", bs[i:])
				return errors.New(m)
			}
			// fmt.Printf("%s\n", string(bs[i:i+n]))
			c.rs = append(c.rs, r)
			i+=n
		}
	}
	return nil
}

func testUtf8ConduitChain() error {

	p := new(Utf8Producer)
	for p.step = rand.Int()%6; p.step == 0; p.step = rand.Int()%6 {}
	p.sample = rand.Int()%2

	c := new(Utf8Consumer)

	pipe := []conduit.Conduit{NewUtf8Conduit()}

	chn := conduit.NewChain(p, pipe, c, small)

	err := chn.Run()
	if len(chn.Errs) > 0 {
		m := fmt.Sprintf("error occurred: %v", chn.Errs)
		return errors.New(m)
	}
	if err != nil {
		m := fmt.Sprintf("unknown error occurred: %v", err)
		return errors.New(m)
	}
	for i, r := range p.rs {
		if r != c.rs[i] {
			m := fmt.Sprintf("source and target differ: %d - %d", r, c.rs[i])
			return errors.New(m)
		}
	}
	// fmt.Printf("%s\n", string(c.rs))
	return nil
}

