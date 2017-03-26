// A simple echo server consisting only of predefined components.
package main

import (
	"fmt"
	"os"
	"github.com/toschoo/conduit"
	cutils "github.com/toschoo/conduit/utils"
)

func main() {
	rdr := cutils.NewTextReader(os.Stdin)
	prn := cutils.NewPrinter(os.Stdout)
	chn := conduit.NewChain(rdr, nil, prn, 10)
	err := chn.Run()
	if err != nil {
		fmt.Printf("%v: %v\n", chn.Errs)
	}
}
