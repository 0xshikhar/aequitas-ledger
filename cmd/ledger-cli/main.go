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

	"golang.org/x/net/websocket"
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
		fmt.Fprintf(os.Stderr, "  rpc-test         Test Ethereum RPC endpoints (HTTP & WS)\n")
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
	case "rpc-test":
		rpcTestCmd(client, args[1:])
	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n", cmd)
		flag.Usage()
		os.Exit(1)
	}
}

func rpcTestCmd(client *http.Client, args []string) {
	fs := flag.NewFlagSet("rpc-test", flag.ExitOnError)
	rpcURL := fs.String("http", "https://ethereum-mainnet-rpc.crouton.digital", "Ethereum JSON-RPC HTTP endpoint")
	wsURL := fs.String("ws", "wss://ethereum-mainnet-ws.crouton.digital", "Ethereum JSON-RPC WebSocket endpoint")
	fs.Parse(args)

	fmt.Printf("=== Testing HTTP RPC Endpoint: %s ===\n", *rpcURL)
	testHTTPRPC(client, *rpcURL)

	fmt.Printf("\n=== Testing WebSocket RPC Endpoint: %s ===\n", *wsURL)
	testWSRPC(*wsURL)
}

func testHTTPRPC(client *http.Client, endpoint string) {
	reqBody := []byte(`{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}`)
	resp, err := client.Post(endpoint, "application/json", bytes.NewReader(reqBody))
	if err != nil {
		fmt.Printf("[HTTP ERROR] Failed to connect: %v\n", err)
		return
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		fmt.Printf("[HTTP ERROR] Failed to read body: %v\n", err)
		return
	}

	var result struct {
		JSONRPC string `json:"jsonrpc"`
		ID      int    `json:"id"`
		Result  string `json:"result"`
		Error   *struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}

	if err := json.Unmarshal(body, &result); err != nil {
		fmt.Printf("[HTTP SUCCESS] Raw Response: %s\n", string(body))
		return
	}

	if result.Error != nil {
		fmt.Printf("[HTTP RPC ERROR] Code %d: %s\n", result.Error.Code, result.Error.Message)
		return
	}

	fmt.Printf("[HTTP SUCCESS] Latest Block Hex: %s\n", result.Result)
}

func testWSRPC(endpoint string) {
	ws, err := websocket.Dial(endpoint, "", "http://localhost/")
	if err != nil {
		fmt.Printf("[WS ERROR] Failed to connect: %v\n", err)
		return
	}
	defer ws.Close()

	fmt.Println("[WS SUCCESS] Connected successfully. Subscribing to newHeads...")
	subReq := `{"jsonrpc":"2.0","id":1,"method":"eth_subscribe","params":["newHeads"]}`
	if _, err := ws.Write([]byte(subReq)); err != nil {
		fmt.Printf("[WS ERROR] Failed to send subscription request: %v\n", err)
		return
	}

	ws.SetReadDeadline(time.Now().Add(5 * time.Second))
	var buf [4096]byte
	n, err := ws.Read(buf[:])
	if err != nil {
		fmt.Printf("[WS ERROR] Failed to receive payload: %v\n", err)
		return
	}

	fmt.Printf("[WS SUCCESS] Received response:\n%s\n", string(buf[:n]))
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
