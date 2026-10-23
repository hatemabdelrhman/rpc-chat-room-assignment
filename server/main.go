package main

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"os/signal"
	"sort"
	"sync"
	"syscall"
	"time"

	"rpc-chat-room/common"
)

// ClientSession stores the message queue and heartbeat time for a connected user.
type ClientSession struct {
	Username string
	Messages []common.ChatMessage
	LastSeen time.Time
}

// ChatServer manages connected clients and synchronizes state access.
type ChatServer struct {
	mu      sync.Mutex
	clients map[string]*ClientSession
}

func NewChatServer() *ChatServer {
	return &ChatServer{
		clients: make(map[string]*ClientSession),
	}
}

// Join registers a new client if the username is unique.
func (s *ChatServer) Join(args *common.JoinArgs, reply *common.JoinReply) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if args.Username == "" {
		reply.Success = false
		reply.Message = "Username cannot be empty."
		return nil
	}

	if _, exists := s.clients[args.Username]; exists {
		reply.Success = false
		reply.Message = fmt.Sprintf("Username '%s' is already taken.", args.Username)
		return nil
	}

	s.clients[args.Username] = &ClientSession{
		Username: args.Username,
		Messages: make([]common.ChatMessage, 0),
		LastSeen: time.Now(),
	}

	log.Printf("[JOIN] User '%s' connected. Total users: %d", args.Username, len(s.clients))

	// Broadcast join notification to all OTHER clients
	s.broadcastLocked(common.ChatMessage{
		Sender:    "Server",
		Content:   fmt.Sprintf("%s has joined the chat.", args.Username),
		Timestamp: time.Now(),
		IsSystem:  true,
	}, args.Username)

	reply.Success = true
	reply.Message = fmt.Sprintf("Welcome to the chat room, %s!", args.Username)
	return nil
}

// SendMessage broadcasts a message from a user to all other connected users.
func (s *ChatServer) SendMessage(args *common.SendMessageArgs, reply *common.SendMessageReply) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	client, exists := s.clients[args.Username]
	if !exists {
		return errors.New("user is not connected to the server")
	}
	client.LastSeen = time.Now()

	log.Printf("[MSG] %s: %s", args.Username, args.Content)

	s.broadcastLocked(common.ChatMessage{
		Sender:    args.Username,
		Content:   args.Content,
		Timestamp: time.Now(),
		IsSystem:  false,
	}, args.Username)

	reply.Success = true
	return nil
}

// FetchMessages returns and clears pending messages for a polling client.
func (s *ChatServer) FetchMessages(args *common.FetchArgs, reply *common.FetchReply) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	client, exists := s.clients[args.Username]
	if !exists {
		return errors.New("user session not found or expired")
	}

	client.LastSeen = time.Now()
	reply.Messages = client.Messages
	// Clear queue after delivering
	client.Messages = make([]common.ChatMessage, 0)
	return nil
}

// ListUsers returns an alphabetically sorted list of active usernames.
func (s *ChatServer) ListUsers(args *common.ListUsersArgs, reply *common.ListUsersReply) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	users := make([]string, 0, len(s.clients))
	for username := range s.clients {
		users = append(users, username)
	}
	sort.Strings(users)
	reply.Users = users
	return nil
}

// Leave cleanly removes a client and notifies others.
func (s *ChatServer) Leave(args *common.LeaveArgs, reply *common.LeaveReply) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.clients[args.Username]; exists {
		delete(s.clients, args.Username)
		log.Printf("[LEAVE] User '%s' disconnected cleanly. Total users: %d", args.Username, len(s.clients))

		s.broadcastLocked(common.ChatMessage{
			Sender:    "Server",
			Content:   fmt.Sprintf("%s has left the chat.", args.Username),
			Timestamp: time.Now(),
			IsSystem:  true,
		}, args.Username)
	}

	reply.Success = true
	return nil
}

// broadcastLocked appends a message to all clients except excludeUser.
func (s *ChatServer) broadcastLocked(msg common.ChatMessage, excludeUser string) {
	for username, session := range s.clients {
		if username == excludeUser {
			continue
		}
		session.Messages = append(session.Messages, msg)
	}
}

// monitorConnections detects unexpected client disconnections.
func (s *ChatServer) monitorConnections(timeout time.Duration) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		s.mu.Lock()
		now := time.Now()
		for username, session := range s.clients {
			if now.Sub(session.LastSeen) > timeout {
				delete(s.clients, username)
				log.Printf("[TIMEOUT] User '%s' timed out. Total users: %d", username, len(s.clients))
				s.broadcastLocked(common.ChatMessage{
					Sender:    "Server",
					Content:   fmt.Sprintf("%s disconnected unexpectedly.", username),
					Timestamp: now,
					IsSystem:  true,
				}, username)
			}
		}
		s.mu.Unlock()
	}
}

func main() {
	port := ":8080"
	if len(os.Args) > 1 {
		port = ":" + os.Args[1]
	}

	chatServer := NewChatServer()
	err := rpc.Register(chatServer)
	if err != nil {
		log.Fatalf("Failed to register RPC server: %v", err)
	}

	listener, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("Failed to listen on %s: %v", port, err)
	}

	// Start background reaper for unexpected disconnections
	go chatServer.monitorConnections(4 * time.Second)

	// Handle graceful server shutdown on Ctrl+C
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		log.Println("\n[SHUTDOWN] Shutting down server gracefully...")
		listener.Close()
		os.Exit(0)
	}()

	log.Printf("RPC Chat Server running on port %s. Press Ctrl+C to stop.", port)

	for {
		conn, err := listener.Accept()
		if err != nil {
			break
		}
		go rpc.ServeConn(conn)
	}
}
