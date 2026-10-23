package main

import (
	"bufio"
	"fmt"
	"net/rpc"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"rpc-chat-room/common"
)

var disconnectOnce sync.Once

func main() {
	serverAddr := "localhost:8080"
	if len(os.Args) > 1 {
		serverAddr = os.Args[1]
	}

	client, err := rpc.Dial("tcp", serverAddr)
	if err != nil {
		fmt.Printf("Error connecting to server at %s: %v\n", serverAddr, err)
		os.Exit(1)
	}
	defer client.Close()

	reader := bufio.NewReader(os.Stdin)
	var username string

	// 1. Prompt for a unique username
	for {
		fmt.Print("Enter your username: ")
		input, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println("\nExiting...")
			return
		}
		username = strings.TrimSpace(input)
		if username == "" {
			fmt.Println("Username cannot be empty. Please try again.")
			continue
		}

		joinArgs := &common.JoinArgs{Username: username}
		var joinReply common.JoinReply
		err = client.Call("ChatServer.Join", joinArgs, &joinReply)
		if err != nil {
			fmt.Printf("RPC Error during Join: %v\n", err)
			return
		}

		if joinReply.Success {
			fmt.Printf("\n--- %s ---\n", joinReply.Message)
			fmt.Println("Commands: '/users' to list connected users, '/exit' to leave.")
			fmt.Println("---------------------------------------------------")
			break
		} else {
			fmt.Printf("[Server]: %s\n", joinReply.Message)
		}
	}

	// Helper for clean disconnect
	cleanDisconnect := func() {
		disconnectOnce.Do(func() {
			leaveArgs := &common.LeaveArgs{Username: username}
			var leaveReply common.LeaveReply
			_ = client.Call("ChatServer.Leave", leaveArgs, &leaveReply)
			client.Close()
			fmt.Println("\nDisconnected from chat server. Goodbye!")
		})
	}

	// 2. Handle Ctrl+C for clean shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		cleanDisconnect()
		os.Exit(0)
	}()

	// 3. Start background polling loop
	stopPolling := make(chan struct{})
	go pollMessages(client, username, stopPolling)

	// 4. Main Terminal Input Loop
	for {
		fmt.Print("> ")
		text, err := reader.ReadString('\n')
		if err != nil {
			close(stopPolling)
			cleanDisconnect()
			break
		}

		content := strings.TrimSpace(text)
		if content == "" {
			continue
		}

		if content == "/exit" || content == "/quit" {
			close(stopPolling)
			cleanDisconnect()
			break
		}

		if content == "/users" || content == "/list" {
			var listReply common.ListUsersReply
			err := client.Call("ChatServer.ListUsers", &common.ListUsersArgs{}, &listReply)
			if err != nil {
				fmt.Printf("\r[Error fetching users]: %v\n", err)
				continue
			}
			fmt.Printf("Connected Users (%d): %s\n", len(listReply.Users), strings.Join(listReply.Users, ", "))
			continue
		}

		// Broadcast message
		sendArgs := &common.SendMessageArgs{
			Username: username,
			Content:  content,
		}
		var sendReply common.SendMessageReply
		err = client.Call("ChatServer.SendMessage", sendArgs, &sendReply)
		if err != nil {
			fmt.Printf("\r[Connection lost]: Failed to send message (%v)\n", err)
			close(stopPolling)
			os.Exit(1)
		}
	}
}

func pollMessages(client *rpc.Client, username string, stop <-chan struct{}) {
	ticker := time.NewTicker(300 * time.Millisecond)
	defer ticker.Stop()

	fetchArgs := &common.FetchArgs{Username: username}

	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			var fetchReply common.FetchReply
			err := client.Call("ChatServer.FetchMessages", fetchArgs, &fetchReply)
			if err != nil {
				fmt.Printf("\r\033[K[System]: Connection to server lost.\n")
				os.Exit(1)
			}

			if len(fetchReply.Messages) > 0 {
				fmt.Print("\r\033[K") // Clear current line
				for _, msg := range fetchReply.Messages {
					timeStr := msg.Timestamp.Format("15:04:05")
					if msg.IsSystem {
						fmt.Printf("[%s] *** %s ***\n", timeStr, msg.Content)
					} else {
						fmt.Printf("[%s] %s: %s\n", timeStr, msg.Sender, msg.Content)
					}
				}
				fmt.Print("> ") // Restore prompt
			}
		}
	}
}
