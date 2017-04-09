// Package utils provides a collection of
// useful and instructive producers, consumers 
// and conduits.
package utils

import (
	"encoding/csv"
	"fmt"
	"github.com/toschoo/conduit"
	"io"
	"unicode/utf8"
)

// Generators are expected to provide an interface
// to generate data that are then fed into the processing chain.
// The Generic Producer uses a user-defined Generator
// to create data.
type Generator interface {
	Generate() (interface{}, error)
}

// Generic is a Producer that uses a Generator
// to create data. It continues calling Generate()
// and sending the result down the chain, util
// Generate() returns io.EOF.
type Generic struct {
	gen Generator
}

// Produce is the pre-defined method that
// makes Generic a Producer.
func (g *Generic) Produce(trg conduit.Target) error {
	for {
		rec, err := g.gen.Generate()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		trg <- rec
	}
	return nil
}

// NewGeneric creates a new Generic Producer
// using a user-defined Generator.
func NewGeneric(gen Generator) (g *Generic) {
	g = new(Generic)
	if g != nil {
		g.gen = gen
	}
	return
}

// Reader is a Producer that feeds data
// read from some kind of source 
// into the processing chain.
type Reader struct {
	rd   io.Reader
	sz   int
}

// Produce is the pre-defined method that
// makes Reader a Producer.
func (rd *Reader) Produce(trg conduit.Target) error {
	for {
		buf := make([]byte,rd.sz)
		n, err := rd.rd.Read(buf)
		if err != nil {
			if err != io.EOF {
				return err
			}
		}
		if n > 0 {
			trg <- buf[:n]
		}
		if err == io.EOF {
			break
		}
	}
	return nil
}

// NewReader creates a new Reader Producer
// using some kind of io.Reader.
// The data is sent down the chain as []byte.
func NewReader(reader io.Reader) (r *Reader) {
	r = new(Reader)
	if r != nil {
		r.rd = reader
		r.sz = 8192
	}
	return
}

// CSV is a Producer that feeds data
// read from a CSV-encoded source 
// line by line into the processing chain.
// CSV releases data as string slices,
// each slice representing 
// one line in the CSV source.
type CSV struct {
	Rd *csv.Reader
}

// Produce is the pre-defined method that
// makes CSV a Producer.
func (p *CSV) Produce(trg conduit.Target) error {
	for {
		rec, err := p.Rd.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		trg <- rec
	}
	return nil
}

// NewCSV creates a new CSV Producer
// using some kind of io.Reader.
func NewCSV (r io.Reader) (p *CSV) {
	p = new(CSV)
	if p != nil {
		p.Rd = csv.NewReader(r)
	}
	return
}

// CSW is a Consumer that feeds data
// read from a CSV-encoded source 
// line by line into the processing chain.
// CSW receives data as string slices,
// each slice representing 
// one line in the CSV target.
type CSW struct {
	Wt *csv.Writer
}

// Consume is the pre-defined method that
// makes CSW a Consumer.
func (csw *CSW) Consume(src conduit.Source) error {
	for inp := range src {
		line := inp.([]string)
		csw.Wt.Write(line)
	}
	csw.Wt.Flush()
	return nil
}

// NewCSW creates a new CSV Consumer
// using some kind of io.Writer.
func NewCSW(stream io.Writer) *CSW {
	csw := new(CSW)
	csw.Wt = csv.NewWriter(stream)
	return csw
}

// Identity is a Conduit that 
// forwards incoming data as is.
// It is useful only as a demonstration
// and testing device. 
type Identity struct{}

// Conduct is the predefined method that makes Identity a Conduit.
func (id *Identity) Conduct(src conduit.Source, trg conduit.Target) error {
	for i := range src {
		trg <- i
	}
	return nil
}

// Creates a new Identity.
func NewIdentity() (id *Identity) {
	id = new(Identity)
	return
}

// Sieves are expected to provide an interface
// to filter incoming data. Only those data that
// pass the filter are then sent down the chain.
// The Filter Conduit uses a user-defined Sieve
// to filter data.
type Sieve interface {
	Sieve(interface{}) bool
}

// Filter is a Conduit that 
// forwards incoming data based on a Sieve.
// Only those data are passed onward that
// pass the filter.
type Filter struct {
	f Sieve
}

// Conduct is the predefined method that makes Filter a Conduit.
func (fil *Filter) Conduct(src conduit.Source, trg conduit.Target) error {
	for inp := range src {
		if fil.f.Sieve(inp) {
			trg <- inp
		}
	}
	return nil
}

// NewFilter creates a new Filter based on a user-defined Sieve.
func NewFilter(filter Sieve) (fil *Filter) {
	fil = new(Filter)
	if fil != nil {
		fil.f = filter
	}
	return
}

// Transforms are expected to provide the Transform method
// that transforms incoming data and sends the result
// down the processing chain. Transforms are used by
// Transformers to implement generic transformation conduits.
type Transform interface {
	Transform(interface{}) (interface{}, error)
}

// Transformer is a conduit that uses a Transform
// to process incoming data. It passes the result
// onward in the processing chain.
// Note that, when Transform returns nil as result, 
// this specific item is skipped
// and processing continues with the next item.
// Transformers, hence, can be used to implement filters.
type Transformer struct {
	t Transform
}

// Conduct is the pre-defined method that makes Transformer a Conduit.
func (trn *Transformer) Conduct(src conduit.Source, trg conduit.Target) error {
	for inp := range src {
		oup, err := trn.t.Transform(inp)
		if err != nil {
			return err
		}
		if oup == nil {
			continue
		}
		trg <- oup
	}
	return nil
}

// NewTransformer creates a new Transformer
// based on a user-defined Transform.
func NewTransformer(trnf Transform) (trn *Transformer) {
	trn = new(Transformer)
	if trn != nil {
		trn.t = trnf
	}
	return
}

// Utf8Conduit receives a stream of bytes
// that represent utf8-encoded runes;
// Utf8Conduit guarantees that each block
// of bytes sent down the processing stream
// contains only full runes, i.e.
// runes are not broken into parts at
// the block barriers.
// If the original stream does not contain
// invalid rune, then Utf8Conduit guarantees
// that the outgoing stream does not contain
// invalid runes either.
type Utf8Conduit struct {
	lo  []byte
	inv []byte
	idx int
}

// Conduct makes Utf8Conduit a Conduit
func (u *Utf8Conduit) Conduct(src conduit.Source, trg conduit.Target) error {
	for inp := range src {

		bs := inp.([]byte)

		n := u.addLeftOver(bs, trg)
		b2 := bs[n:]

		l := u.storeLeftOver(b2)
		if len(b2[:l]) > 0 {
			trg <- b2[:l]
		}
	}
	return nil
}

// NewUtf8Conduit creates a new conduit
func NewUtf8Conduit() *Utf8Conduit {
	u := new(Utf8Conduit)
	u.lo = make([]byte,utf8.UTFMax)
	u.inv = make([]byte,3)
	_ = utf8.EncodeRune(u.inv, utf8.RuneError)
	return u
}

// helper for Utf8Conduit that stores leftover bytes,
// i.e. bytes at the end of the buffer that do not
//      form a valid rune
func (u *Utf8Conduit) storeLeftOver(bs []byte) int {

	s := len(bs)

	// buf is empty: nothing to do
	if s == 0 {
		return 0
	}

	r, _ := utf8.DecodeLastRune(bs)

	// last rune complete: nothing to do
	if r != utf8.RuneError {
		return s
	}

	l := s-1

	// buf has only one element, grab it
	if l == 0 {
		u.lo[u.idx] = bs[l]
		u.idx++
		return s-1
	}

	// go backwards in the buf, until we find
	// a valid rune (at most UTFMax byte); 
	// the bytes ahead of the end
	// of that rune are leftovers. 
	min := s-utf8.UTFMax
	for ; l >= 0 && l > min; l-- {
		r, _ := utf8.DecodeLastRune(bs[:l])
		if r != utf8.RuneError {
			for i:=l; i<s; i++ {
				u.lo[u.idx] = bs[i]
				u.idx++
			}
			return l
		}
	}
	// invalid utf
	if u.idx == utf8.UTFMax {
		return s
	}
	// buffer < utf8.UTFMax
	l++
	for i:=l; i<s; i++ {
		u.lo[u.idx] = bs[i]
		u.idx++
	}
	return l
}

// helper for Utf8Conduit that completes runes
// using the leftover bytes
func (u *Utf8Conduit) addLeftOver(bs []byte, trg conduit.Target) int {

	// no leftovers
	if u.idx == 0 {
		return 0
	}

	// start with the leftovers
	tmp := u.lo[:u.idx]

	i:=0
	s := len(bs)

	// add bytes until we have a valid rune,
	// at most UTFMax
	for ; i<s && u.idx < utf8.UTFMax; i++ {

		tmp = append(tmp,bs[i])

		// complete: send it
		if utf8.FullRune(tmp) {
			trg <- tmp
			u.idx = 0
			return i+1
		}

		// remember for the case
		// the buffer is smaller 
		// than UTF8Max
		u.idx++;
	}
	// invalid rune
	if u.idx == utf8.UTFMax {
		trg <- u.inv
		u.idx = 0
		return i
	}

	// ignore entire buffer
	return s
}

// Printer is a Consumer that writes the result
// as text to some kind of io.Writer using fmt.Fprintf.
type Printer struct {
	stream io.Writer
	text   bool
}

// Consume is the pre-defined method 
// that makes Printer a Consumer.
func (prn *Printer) Consume(src conduit.Source) error {
	for inp := range src {
		if prn.text {
			buf := inp.([]byte)
			runes := string(buf)
			fmt.Fprintf(prn.stream, "%s\n", runes)
		} else {
			fmt.Fprintf(prn.stream, "%v\n", inp)
		}
	}
	return nil
}

// NewPrinter creates a new Printer Consumer
// that writes the data using fmt.Fprintf with verb "%v".
func NewPrinter(stream io.Writer) (p *Printer) {
	p = new(Printer)
	if p != nil {
		p.stream = stream
		p.text   = false
	}
	return
}

// NewTextPrinter creates a new Printer Consumer
// that writes the data using fmt.Fprintf with verb %s.
// TextPrinter expects incoming data to be byte slices, 
// which are explictly converted to strings.
// Note that TextPrinter does not ensure that
// the arriving byte slices contain correctly encoded
// unicode. That must be ensured by the producer or
// a conduit in the processing chain.
func NewTextPrinter(stream io.Writer) (p *Printer) {
	p = NewPrinter(stream)
	if p != nil {
		p.text = true
	}
	return
}
