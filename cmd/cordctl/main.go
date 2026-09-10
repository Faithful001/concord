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

	linearizableFlag := flag.Bool("linearizable", false, "Ensure strongly consistent / linearizable read via ReadIndex protocol")
	flag.BoolVar(linearizableFlag, "l", false, "Shorthand for --linearizable")

	learnerFlag := flag.Bool("learner", false, "Add node as a non-voting read-only learner replica")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: cordctl [options] <command> [args...]\n\n")
		fmt.Fprintf(os.Stderr, "Commands:\n")
		fmt.Fprintf(os.Stderr, "  put <key> <value>                    Write a key-value pair\n")
		fmt.Fprintf(os.Stderr, "  get <key> [-l|--linearizable]        Read the value for a key (optional linearizable read)\n")
		fmt.Fprintf(os.Stderr, "  del <key>                            Delete a key\n")
		fmt.Fprintf(os.Stderr, "  status                               Check cluster status and leader info\n")
		fmt.Fprintf(os.Stderr, "  health                               Check node health\n")
		fmt.Fprintf(os.Stderr, "  member list                          List all cluster members\n")
		fmt.Fprintf(os.Stderr, "  member add <id> <raft> [api] [--learner]  Add a new node or learner replica to the cluster\n")
		fmt.Fprintf(os.Stderr, "  member remove <id>                   Remove a node from the cluster\n\n")
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

	client := &http.Client{Timeout: 6 * time.Second}

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
			fmt.Fprintf(os.Stderr, "Error: 'get' requires <key>\nUsage: cordctl get <key> [-l|--linearizable]\n")
			os.Exit(1)
		}
		key := args[1]
		// check if trailing flags were provided after command
		isLin := *linearizableFlag
		for _, a := range args[2:] {
			if a == "-l" || a == "--linearizable" {
				isLin = true
			}
		}
		getKey(client, endpoint, key, isLin)

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

	case "member", "members":
		if len(args) < 2 {
			fmt.Fprintf(os.Stderr, "Error: 'member' subcommand required (list, add, remove)\n")
			os.Exit(1)
		}
		switch args[1] {
		case "list", "ls":
			listMembers(client, endpoint)
		case "add":
			if len(args) < 4 {
				fmt.Fprintf(os.Stderr, "Error: 'member add' requires <id> and <raft_addr>\nUsage: cordctl member add <id> <raft_addr> [api_addr] [--learner]\n")
				os.Exit(1)
			}
			id, raftAddr := args[2], args[3]
			apiAddr := ""
			isLearner := *learnerFlag
			for _, a := range args[4:] {
				if a == "--learner" {
					isLearner = true
				} else if !strings.HasPrefix(a, "-") && apiAddr == "" {
					apiAddr = a
				}
			}
			addMember(client, endpoint, id, raftAddr, apiAddr, isLearner)
		case "remove", "rm", "del", "delete":
			if len(args) < 3 {
				fmt.Fprintf(os.Stderr, "Error: 'member remove' requires <id>\nUsage: cordctl member remove <id>\n")
				os.Exit(1)
			}
			id := args[2]
			removeMember(client, endpoint, id)
		default:
			fmt.Fprintf(os.Stderr, "Unknown member subcommand: %s\n", args[1])
			os.Exit(1)
		}

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

func getKey(client *http.Client, endpoint, key string, linearizable bool) {
	url := fmt.Sprintf("%s/v1/kv/%s", endpoint, key)
	if linearizable {
		url += "?linearizable=true"
	}
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

func listMembers(client *http.Client, endpoint string) {
	url := fmt.Sprintf("%s/v1/members", endpoint)
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

func addMember(client *http.Client, endpoint, id, raftAddr, apiAddr string, isLearner bool) {
	url := fmt.Sprintf("%s/v1/members", endpoint)
	payload := map[string]any{
		"id":         id,
		"raft_addr":  raftAddr,
		"api_addr":   apiAddr,
		"is_learner": isLearner,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to encode request: %v\n", err)
		os.Exit(1)
	}

	resp, err := client.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Request error: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		roleType := "member"
		if isLearner {
			roleType = "learner replica"
		}
		fmt.Printf("Cluster %s %s added successfully.\n", roleType, id)
	} else {
		fmt.Fprintf(os.Stderr, "Error (%d): %s\n", resp.StatusCode, string(respBody))
		os.Exit(1)
	}
}

func removeMember(client *http.Client, endpoint, id string) {
	url := fmt.Sprintf("%s/v1/members/%s", endpoint, id)
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

	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		fmt.Printf("Member %s removed successfully.\n", id)
	} else {
		fmt.Fprintf(os.Stderr, "Error (%d): %s\n", resp.StatusCode, string(respBody))
		os.Exit(1)
	}
}
