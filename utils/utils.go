// Package utils provides a collection of
// useful and instructive producers, consumers 
// and conduits.
package utils

import (
	"github.com/toschoo/conduit"
	"encoding/csv"
	"fmt"
	"io"
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
	text bool
}

// Produce is the pre-defined method that
// makes Reader a Producer.
func (rd *Reader) Produce(trg conduit.Target) error {
	buf := make([]byte,rd.sz)
	for {
		n, err := rd.rd.Read(buf)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		if rd.text {
			trg <- string(buf[:n])
		} else {
			trg <- buf[:n]
		}
	}
	return nil
}

// NewByteReader creates a new Reader Producer
// using some kind of io.Reader.
// The data is sent down the chain as []byte.
func NewByteReader(reader io.Reader) (r *Reader) {
	r = new(Reader)
	if r != nil {
		r.rd = reader
		r.sz = 8192
		r.text = false
	}
	return
}

// NewTextReader creates a new Reader Producer
// using some kind of io.Reader.
// The data is sent down the chain as string.
func NewTextReader(reader io.Reader) (r *Reader) {
	r = NewByteReader(reader)
	if r != nil {
		r.text = true
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
// that writes the data using fmt.Fprintf with ver %s.
// TextPrinter expects incoming data to be byte slices, 
// which are explictly converted to strings.
func NewTextPrinter(stream io.Writer) (p *Printer) {
	p = NewPrinter(stream)
	if p != nil {
		p.text = true
	}
	return
}
