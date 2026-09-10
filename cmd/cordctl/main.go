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
)

type setRequest struct {
	Value string `json:"value"`
}

type getResponse struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func main() {
	endpointFlag := flag.String("endpoint", "", "Concord API endpoint URL (default: http://localhost:9001 or $CORDCTL_ENDPOINT)")
	flag.StringVar(endpointFlag, "e", "", "Shorthand for --endpoint")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: cordctl [options] <command> [args...]\n\n")
		fmt.Fprintf(os.Stderr, "Commands:\n")
		fmt.Fprintf(os.Stderr, "  put <key> <value>    Write a key-value pair\n")
		fmt.Fprintf(os.Stderr, "  get <key>            Read the value for a key\n")
		fmt.Fprintf(os.Stderr, "  del <key>            Delete a key\n")
		fmt.Fprintf(os.Stderr, "  status               Check cluster status and leader info\n")
		fmt.Fprintf(os.Stderr, "  health               Check node health\n\n")
		fmt.Fprintf(os.Stderr, "Options:\n")
		flag.PrintDefaults()
	}

	flag.Parse()

	args := flag.Args()
	if len(args) == 0 {
		flag.Usage()
		os.Exit(1)
	}

	endpoint := *endpointFlag
	if endpoint == "" {
		endpoint = os.Getenv("CORDCTL_ENDPOINT")
	}
	if endpoint == "" {
		endpoint = "http://localhost:9001"
	}
	endpoint = strings.TrimRight(endpoint, "/")

	client := &http.Client{Timeout: 5 * time.Second}

	switch args[0] {
	case "put", "set":
		if len(args) < 3 {
			fmt.Fprintf(os.Stderr, "Error: 'put' requires <key> and <value>\nUsage: cordctl put <key> <value>\n")
			os.Exit(1)
		}
		key, val := args[1], args[2]
		putKey(client, endpoint, key, val)

	case "get":
		if len(args) < 2 {
			fmt.Fprintf(os.Stderr, "Error: 'get' requires <key>\nUsage: cordctl get <key>\n")
			os.Exit(1)
		}
		key := args[1]
		getKey(client, endpoint, key)

	case "del", "delete", "rm":
		if len(args) < 2 {
			fmt.Fprintf(os.Stderr, "Error: 'del' requires <key>\nUsage: cordctl del <key>\n")
			os.Exit(1)
		}
		key := args[1]
		delKey(client, endpoint, key)

	case "status":
		getStatus(client, endpoint)

	case "health", "healthz":
		getHealth(client, endpoint)

	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n", args[0])
		flag.Usage()
		os.Exit(1)
	}
}

func putKey(client *http.Client, endpoint, key, val string) {
	url := fmt.Sprintf("%s/v1/kv/%s", endpoint, key)
	body, err := json.Marshal(setRequest{Value: val})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to encode request: %v\n", err)
		os.Exit(1)
	}

	req, err := http.NewRequest(http.MethodPut, url, bytes.NewReader(body))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create request: %v\n", err)
		os.Exit(1)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Request error: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		fmt.Println("OK")
	} else {
		respBody, _ := io.ReadAll(resp.Body)
		fmt.Fprintf(os.Stderr, "Error (%d): %s\n", resp.StatusCode, string(respBody))
		os.Exit(1)
	}
}

func getKey(client *http.Client, endpoint, key string) {
	url := fmt.Sprintf("%s/v1/kv/%s", endpoint, key)
	resp, err := client.Get(url)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Request error: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		fmt.Fprintf(os.Stderr, "Error: key not found\n")
		os.Exit(1)
	}

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		fmt.Fprintf(os.Stderr, "Error (%d): %s\n", resp.StatusCode, string(respBody))
		os.Exit(1)
	}

	var res getResponse
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to decode response: %v\n", err)
		os.Exit(1)
	}
	fmt.Println(res.Value)
}

func delKey(client *http.Client, endpoint, key string) {
	url := fmt.Sprintf("%s/v1/kv/%s", endpoint, key)
	req, err := http.NewRequest(http.MethodDelete, url, nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create request: %v\n", err)
		os.Exit(1)
	}

	resp, err := client.Do(req)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Request error: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		fmt.Println("OK")
	} else {
		respBody, _ := io.ReadAll(resp.Body)
		fmt.Fprintf(os.Stderr, "Error (%d): %s\n", resp.StatusCode, string(respBody))
		os.Exit(1)
	}
}

func getStatus(client *http.Client, endpoint string) {
	url := fmt.Sprintf("%s/v1/status", endpoint)
	resp, err := client.Get(url)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Request error: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to read response: %v\n", err)
		os.Exit(1)
	}

	var pretty bytes.Buffer
	if err := json.Indent(&pretty, body, "", "  "); err == nil {
		fmt.Println(pretty.String())
	} else {
		fmt.Println(string(body))
	}
}

func getHealth(client *http.Client, endpoint string) {
	url := fmt.Sprintf("%s/healthz", endpoint)
	resp, err := client.Get(url)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Request error: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode == http.StatusOK {
		fmt.Println("OK -", string(body))
	} else {
		fmt.Fprintf(os.Stderr, "Unhealthy (%d): %s\n", resp.StatusCode, string(body))
		os.Exit(1)
	}
}
