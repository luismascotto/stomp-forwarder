package forwarder

import (
	"errors"
	"fmt"
	"regexp"
	"time"
)

type Configuration struct {
	FromAmqpFullUri    string `json:"fromAmqpFullUri"`
	ToAmqpFullUri      string `json:"toAmqpFullUri"`
	FromDestination    string `json:"fromDestination"`
	ToDestination      string `json:"toDestination"`
	SupressLogRepeater bool   `json:"supressLogRepeater"`
	SupressLogSender   bool   `json:"supressLogSender"`
	SupressLogReceiver bool   `json:"supressLogReceiver"`
	StartAsync         bool   `json:"startAsync"`
}

type EventMessage struct {
	EventType string `json:"eventType"`
	EventData string `json:"eventData"`
}

const (
	// When reconnecting to the server after connection failure
	ReconnectDelay = 2 * time.Second

	// When setting up the topic after a connectio exception
	ResubscribeDelay = 2 * time.Second
)

func extractUserPassHost(fullUri string) (string, string, string, error) {
	regex := regexp.MustCompile(`stomp://([^:]+):([^@]+)@([^:]+):(\d+)`)
	matches := regex.FindStringSubmatch(fullUri)

	if len(matches) != 5 {
		fmt.Println("Invalid URI format. Expected format: stomp://username:password@host:port")
		return "", "", "", errors.New("invalid uri format")
	}

	user := matches[1]
	pass := matches[2]
	host := matches[3] + ":" + matches[4]

	return user, pass, host, nil
}

var (
	Reset   = "\033[0m"
	Red     = "\033[31m"
	Green   = "\033[32m"
	Yellow  = "\033[33m"
	Blue    = "\033[34m"
	Magenta = "\033[35m"
	Cyan    = "\033[36m"
	Gray    = "\033[37m"
	White   = "\033[97m"
)
