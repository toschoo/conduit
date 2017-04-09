// A simple unicode-aware echo server 
// consisting only of predefined components.
package main

import (
	"fmt"
	"os"
	"github.com/toschoo/conduit"
	cutils "github.com/toschoo/conduit/utils"
)

func main() {
	rdr := cutils.NewReader(os.Stdin)
	pipe := []conduit.Conduit{cutils.NewUtf8Conduit()}
	prn := cutils.NewTextPrinter(os.Stdout)
	chn := conduit.NewChain(rdr, pipe, prn, 10)
	err := chn.Run()
	if err != nil {
		fmt.Printf("%v: %v\n", chn.Errs)
	}
}
