package main

import (
	"crypto/sha1"
	"fmt"
	"github.com/spf13/cobra"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"
)

var id string
var host string
var port[] int
var secret []byte
var root string

var rootCommand = &cobra.Command{
	Use:   "bdb",
	Short: "BantamDB",
	Run:   run,
}

func main() {
	i := GetRandomString(16)
	rootCommand.Flags().StringVarP(&id, "id", "i", i, "The node identifier")

	h, err := os.Hostname()
	if err != nil || h == "" {
		h = "localhost"
	}
	rootCommand.Flags().StringVarP(&id, "host", "h", h, "The node hostname")

	p := "8080:10"
	rootCommand.Flags().StringVarP(&p, "ports", "p", p, "The port range assigned in the form port:count")
	ps := strings.Split(p, ":")
	pf, _ := strconv.Atoi(ps[0])
	pc, _ := strconv.Atoi(ps[1])
	port = make([]int, pc)
	for r := pf; r < pf + pc; r++ {
		port = append(port, r)
	}

	s := ""
	rootCommand.Flags().StringVarP(&s, "secret", "s", "", "The secret for cluster membership")
	b := sha1.Sum([]byte(s))
	secret = b[:]

	rootCommand.Flags().StringVarP(&root, "root", "r", "", "The root path for local storage")
	if root == "" {
		r, err := ioutil.TempDir(id, "")
		if err != nil || r == "" {
			log.Fatal("Unable to initialize local storage")
		}
	}

	if err := rootCommand.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(cmd *cobra.Command, _ []string) {

}

