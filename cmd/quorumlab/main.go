package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"quorumlab/simulator"
)

func run(args []string) int {
	if len(args) < 1 || (args[0] != "demo" && args[0] != "evaluate") {
		fmt.Fprintln(os.Stderr, "usage: quorumlab demo|evaluate [--output PATH]")
		return 2
	}
	options := flag.NewFlagSet(args[0], flag.ContinueOnError)
	output := options.String("output", "", "write JSON report")
	if options.Parse(args[1:]) != nil || options.NArg() != 0 {
		return 2
	}
	var report any
	passed := true
	if args[0] == "demo" {
		result := simulator.Scenario(3, 7)
		report = result
		passed = result.Passed
	} else {
		result := simulator.Evaluate()
		report = result
		passed = result.Passed == result.Total
	}
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		fmt.Fprintln(os.Stderr, "report_encoding_failed")
		return 2
	}
	data = append(data, '\n')
	if *output != "" {
		if err = os.MkdirAll(filepath.Dir(*output), 0755); err == nil {
			err = os.WriteFile(*output, data, 0644)
		}
		if err != nil {
			fmt.Fprintln(os.Stderr, "report_write_failed")
			return 2
		}
	} else {
		fmt.Print(string(data))
	}
	if !passed {
		return 1
	}
	return 0
}
func main() { os.Exit(run(os.Args[1:])) }
