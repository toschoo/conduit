// Prints the squares.
package main

import (
	"fmt"
	"io"
	"os"
	"github.com/toschoo/conduit"
	cutils "github.com/toschoo/conduit/utils"
)

// ------------------------------------------------------------------------
// Counter
// ------------------------------------------------------------------------
type Counter struct{
	max, cur int
}

func (cnt *Counter) Generate() (interface{}, error) {
	if cnt.cur >= cnt.max {
		return nil, io.EOF
	}
	tmp := cnt.cur
	cnt.cur++
	return tmp, nil
}

func NewCounter(min, max int) (cnt *Counter) {
	cnt = new(Counter)
	if cnt != nil {
		cnt.cur = min
		cnt.max = max
	}
	return
}

// ------------------------------------------------------------------------
// Squarer
// ------------------------------------------------------------------------
type SqrConduit struct{}

func (sqr *SqrConduit) Conduct(src conduit.Source, trg conduit.Target) error {
	for x := range src {
		n := x.(int)
		trg <- (n * n)
	}
	return nil
}

// ------------------------------------------------------------------------
// Running the chain
// ------------------------------------------------------------------------
func main() {
	cnt := cutils.NewGeneric(NewCounter(0, 32))
	prn := cutils.NewPrinter(os.Stdout)
	pipe := []conduit.Conduit{new(SqrConduit)}
	chn := conduit.NewChain(cnt, pipe, prn, 10)
	err := chn.Run()
	if err != nil {
		fmt.Printf("%v: %v\n", err, chn.Errs)
	}
}
