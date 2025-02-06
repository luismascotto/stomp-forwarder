package forwarder

import (
	"fmt"
	"log"
	"time"
)

type MessageRepeater struct {
	Reader     *MessageConsumer
	Writer     *MessageProducer
	stopch     chan struct{}
	exitch     chan struct{}
	supressLog bool
}

func NewMessageRepeater(config *Configuration) *MessageRepeater {
	reader := NewMessageConsumer(config.FromAmqpFullUri, config.FromDestination, config.SupressLogReceiver)
	if reader == nil {
		fmt.Println("Error creating consumer")
		return nil
	}
	writer := NewMessageProducer(config.ToAmqpFullUri, config.ToDestination, config.SupressLogSender)
	if writer == nil {
		fmt.Println("Error creating producer")
		return nil
	}

	messageRepeater := &MessageRepeater{
		Reader:     reader,
		Writer:     writer,
		stopch:     make(chan struct{}),
		exitch:     make(chan struct{}),
		supressLog: config.SupressLogRepeater,
	}

	return messageRepeater
}

func (mr *MessageRepeater) Start() {
	go mr.Writer.Start()
	go mr.Reader.Start()

	mr.sendMsgToLog("Waiting for connections")
	startTime := time.Now()
WaitFullConnection:
	for {
		select {
		case <-mr.stopch:
			return
		default:
			if mr.Reader.IsConnected() && mr.Writer.IsConnected() {
				break WaitFullConnection
			}
			if time.Since(startTime) > 30*time.Second {
				mr.sendMsgToLog("Timeout waiting for both connections to be established")
				mr.Reader.Shutdown()
				mr.Writer.Shutdown()
				return
			}
			mr.sendMsgToLog("@")
			time.Sleep(500 * time.Millisecond)
		}
	}

	mr.Broadcast()
}

func (mr *MessageRepeater) Broadcast() {
	mr.sendMsgToLog("Starting Broadcast...")
	for {
		select {
		case <-mr.stopch:
			return
		default:
			message, err := mr.Reader.Receive()
			if err != nil {
				mr.sendMsgToLog(fmt.Sprintf("Error receiving message: %s", err.Error()))
				return
			}
			time.Sleep(50 * time.Millisecond)
			err = mr.Writer.Send(message)
			if err != nil {
				mr.sendMsgToLog(fmt.Sprintf("Error sending message: %s", err.Error()))
				return
			}
			mr.sendMsgToLog("->")
		}
	}
}
func (mr *MessageRepeater) Shutdown() {
	mr.stopch <- struct{}{}
	mr.Reader.Shutdown()
	mr.Writer.Shutdown()
	mr.exitch <- struct{}{}
}

func (mr *MessageRepeater) sendMsgToLog(msg string) {
	if mr.supressLog {
		return
	}
	log.Printf("MR: %s%s%s\n", Yellow, msg, Reset)
}
