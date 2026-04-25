package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"aequitas-ledger/internal/audit"
)

var (
	apiURL = flag.String("url", "http://localhost:8080", "Base URL for the ledger API")
)

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Aequitas Ledger Operator CLI\n\n")
		fmt.Fprintf(os.Stderr, "Usage:\n")
		fmt.Fprintf(os.Stderr, "  ledger-cli [global flags] <command> [subcommand] [flags]\n\n")
		fmt.Fprintf(os.Stderr, "Global Flags:\n")
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\nCommands:\n")
		fmt.Fprintf(os.Stderr, "  account create   Create a new account\n")
		fmt.Fprintf(os.Stderr, "  account get      Get account details by ID\n")
		fmt.Fprintf(os.Stderr, "  transfer create  Create a new transfer between accounts\n")
		fmt.Fprintf(os.Stderr, "  info             Get ledger health status\n")
		fmt.Fprintf(os.Stderr, "  audit            Independently verify a WAL directory (T3.4)\n")
	}

	flag.Parse()
	args := flag.Args()

	if len(args) == 0 {
		flag.Usage()
		os.Exit(1)
	}

	cmd := args[0]
	client := &http.Client{Timeout: 5 * time.Second}

	switch cmd {
	case "account":
		if len(args) < 2 {
			fmt.Fprintln(os.Stderr, "Expected 'create' or 'get' subcommand for 'account'")
			os.Exit(1)
		}
		subCmd := args[1]
		accountCmd(client, subCmd, args[2:])
	case "transfer":
		if len(args) < 2 || args[1] != "create" {
			fmt.Fprintln(os.Stderr, "Expected 'create' subcommand for 'transfer'")
			os.Exit(1)
		}
		transferCmd(client, args[2:])
	case "info":
		infoCmd(client)
	case "audit":
		auditCmd(args[1:])
	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n", cmd)
		flag.Usage()
		os.Exit(1)
	}
}

func accountCmd(client *http.Client, subCmd string, args []string) {
	fs := flag.NewFlagSet("account "+subCmd, flag.ExitOnError)
	switch subCmd {
	case "create":
		id := fs.String("id", "", "Account ID (hex)")
		currency := fs.String("currency", "USD", "Currency code (max 4 chars)")
		initialCredits := fs.String("initial-credits", "0", "Initial credits")
		fs.Parse(args)

		if *id == "" {
			fmt.Fprintln(os.Stderr, "Flag -id is required")
			os.Exit(1)
		}

		payload := map[string]string{
			"id":              *id,
			"currency":        *currency,
			"initial_credits": *initialCredits,
		}
		doPost(client, "/v1/accounts", payload)

	case "get":
		id := fs.String("id", "", "Account ID (hex)")
		fs.Parse(args)

		if *id == "" {
			fmt.Fprintln(os.Stderr, "Flag -id is required")
			os.Exit(1)
		}
		doGet(client, "/v1/accounts/"+*id)
	default:
		fmt.Fprintf(os.Stderr, "Unknown account subcommand: %s\n", subCmd)
		os.Exit(1)
	}
}

func transferCmd(client *http.Client, args []string) {
	fs := flag.NewFlagSet("transfer create", flag.ExitOnError)
	id := fs.String("id", "", "Transfer ID (hex)")
	debitID := fs.String("debit", "", "Debit Account ID (hex)")
	creditID := fs.String("credit", "", "Credit Account ID (hex)")
	amount := fs.String("amount", "", "Transfer Amount")
	idempKey := fs.String("idempotency-key", "", "Idempotency Key (optional)")
	fs.Parse(args)

	if *id == "" || *debitID == "" || *creditID == "" || *amount == "" {
		fmt.Fprintln(os.Stderr, "Flags -id, -debit, -credit, and -amount are required")
		os.Exit(1)
	}

	payload := map[string]string{
		"id":                *id,
		"debit_account_id":  *debitID,
		"credit_account_id": *creditID,
		"amount":            *amount,
		"idempotency_key":   *idempKey,
	}
	doPost(client, "/v1/transfers", payload)
}

func infoCmd(client *http.Client) {
	doGet(client, "/healthz")
}

func doPost(client *http.Client, path string, payload interface{}) {
	b, err := json.Marshal(payload)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error marshaling request: %v\n", err)
		os.Exit(1)
	}

	url := strings.TrimRight(*apiURL, "/") + path
	resp, err := client.Post(url, "application/json", bytes.NewReader(b))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Request failed: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	printResponse(resp)
}

func doGet(client *http.Client, path string) {
	url := strings.TrimRight(*apiURL, "/") + path
	resp, err := client.Get(url)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Request failed: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	printResponse(resp)
}

func printResponse(resp *http.Response) {
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading response: %v\n", err)
		os.Exit(1)
	}

	if resp.StatusCode >= 400 {
		fmt.Fprintf(os.Stderr, "Error: %s (Status %d)\n", string(body), resp.StatusCode)
		os.Exit(1)
	}

	var prettyJSON bytes.Buffer
	if err := json.Indent(&prettyJSON, body, "", "  "); err != nil {
		fmt.Println(string(body))
	} else {
		fmt.Println(prettyJSON.String())
	}
}

// auditCmd runs the independent WAL auditor (T3.4): it re-implements the
// frame format, batch atomicity, LSN ordering, and conservation checks with
// code that shares nothing with the engine's recovery, and exits non-zero if
// any violation is found.
func auditCmd(args []string) {
	fs := flag.NewFlagSet("audit", flag.ExitOnError)
	walDir := fs.String("wal", "", "path to the WAL directory to audit")
	fs.Parse(args)
	if *walDir == "" {
		fmt.Fprintln(os.Stderr, "audit requires --wal <dir>")
		os.Exit(1)
	}

	rep, err := audit.Audit(*walDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "audit failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("WAL audit: %s\n", *walDir)
	fmt.Printf("  segments:   %d\n", rep.Segments)
	fmt.Printf("  records:    %d\n", rep.Records)
	fmt.Printf("  batches:    %d\n", rep.Batches)
	fmt.Printf("  accounts:   %d\n", rep.Accounts)
	fmt.Printf("  transfers:  %d\n", rep.Transfers)
	if rep.TornTail != nil {
		fmt.Printf("  torn tail:  %s\n", rep.TornTail.Detail)
	}
	if rep.Valid() {
		fmt.Println("  result:     PASS — no violations")
		return
	}
	fmt.Printf("  result:     FAIL — %d violation(s)\n", len(rep.Violations))
	for _, v := range rep.Violations {
		fmt.Printf("    - %s\n", v)
	}
	os.Exit(1)
}
