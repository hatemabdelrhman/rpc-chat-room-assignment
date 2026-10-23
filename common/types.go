package common

import "time"

// ChatMessage represents a single chat message or system notification.
type ChatMessage struct {
	Sender    string
	Content   string
	Timestamp time.Time
	IsSystem  bool
}

// JoinArgs holds the username for joining the chat.
type JoinArgs struct {
	Username string
}

// JoinReply informs the client if the join succeeded.
type JoinReply struct {
	Success bool
	Message string
}

// SendMessageArgs contains the sender and message text.
type SendMessageArgs struct {
	Username string
	Content  string
}

// SendMessageReply confirms message delivery to the server.
type SendMessageReply struct {
	Success bool
}

// FetchArgs requests pending messages for a specific user.
type FetchArgs struct {
	Username string
}

// FetchReply returns all queued messages for the polling client.
type FetchReply struct {
	Messages []ChatMessage
}

// ListUsersArgs is an empty struct for requesting active users.
type ListUsersArgs struct{}

// ListUsersReply returns the slice of currently connected usernames.
type ListUsersReply struct {
	Users []string
}

// LeaveArgs holds the username of the disconnecting client.
type LeaveArgs struct {
	Username string
}

// LeaveReply confirms the user was removed.
type LeaveReply struct {
	Success bool
}
