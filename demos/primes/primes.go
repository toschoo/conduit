// Uses a filter to implement the Sieve of Eratosthenes.
package main

import (
	"fmt"
	"io"
	"os"
	"github.com/toschoo/conduit"
	cutils "github.com/toschoo/conduit/utils"
)

// Euclidean Algorithm
func gcd(a, b uint64) uint64 {
	if b == 0 {
		return a
	}
	if a < b {
		return gcd(b, a)
	}
	return gcd(b, a%b)
}

// Eratosthenes is a Sieve
type Erato struct {
	primes []uint64
}

// since it implements the Sieve method.
func (sv *Erato) Sieve(inp interface{}) bool {
	n := inp.(uint64)
	if n < 2 {
		return false
	}
	for _, p := range sv.primes {
		if (p * p) > n {
			break
		}
		if gcd(n, p) != 1 {
			return false
		}
	}
	sv.primes = append(sv.primes, n)
	return true
}

// Counter is a Generic producer
type Counter struct {
	cur, max uint64
}

// since it implements the Generate method
func (cnt *Counter) Generate() (interface{}, error) {
	if cnt.cur >= cnt.max {
		return nil, io.EOF
	}
	tmp := cnt.cur
	cnt.cur++
	return tmp, nil
}

func NewCounter(min, max uint64) (cnt *Counter) {
	cnt = new(Counter)
	if cnt != nil {
		cnt.cur = min
		cnt.max = max
	}
	return
}

// Run the chain
func main() {
	var sv Erato
	pipe := []conduit.Conduit{cutils.NewFilter(&sv)}
	cnt := cutils.NewGeneric(NewCounter(2, 1000))
	prn := cutils.NewPrinter(os.Stdout)
	chn := conduit.NewChain(cnt, pipe, prn, 10)
	err := chn.Run()
	if err != nil {
		fmt.Printf("%v: %v\n", chn.Errs)
	}
}
