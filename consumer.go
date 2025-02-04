package forwarder

import (
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/go-stomp/stomp/v3"
)

type MessageConsumer struct {
	amqpURI         string
	amqpUser        string
	amqpPass        string
	amqpDestination string

	connectionAMQP   *stomp.Conn
	subscriptionAMQP *stomp.Subscription

	shutdown chan struct{}

	notifyConnClose chan *stomp.Error

	mux       sync.Mutex
	connected bool //indica se esta conectado

	supressLog bool

	chEventMessage chan *EventMessage
}

func NewMessageConsumer(amqpFullURI, amqpDestination string, supressLog bool) *MessageConsumer {
	user, pass, host, err := extractUserPassHost(amqpFullURI)
	if err != nil {
		fmt.Println("Consumer: Error extracting user, pass, host from URI")
		return nil
	}

	return newMessageConsumer(host, user, pass, amqpDestination, supressLog)
}

func newMessageConsumer(amqpURI, amqpUser, amqpPass, amqpDestination string, supressLog bool) *MessageConsumer {
	messageConsumer := &MessageConsumer{
		amqpURI:         amqpURI,
		amqpUser:        amqpUser,
		amqpPass:        amqpPass,
		amqpDestination: amqpDestination,
		shutdown:        make(chan struct{}),
		chEventMessage:  make(chan *EventMessage, 1023),
		supressLog:      supressLog,
	}

	return messageConsumer
}

// Start inicia a sessao amqp
func (mc *MessageConsumer) Start() {
	mc.handleReconnectAMQP()
}

func (mc *MessageConsumer) Receive() (*EventMessage, error) {
	if mc.chEventMessage == nil {
		return nil, fmt.Errorf("not initialized")
	}
	if !mc.IsConnected() {
		return nil, fmt.Errorf("not connected")
	}
	if msg, ok := <-mc.chEventMessage; ok {
		return msg, nil
	}
	return nil, fmt.Errorf("channel closed")
}

// handleReconnect will wait for a connection error on
// notifyConnClose, and then continuously attempt to reconnect.
func (mc *MessageConsumer) handleReconnectAMQP() {
	var err error
	var connectionErrors int = 1
	for {
		mc.sendMsgToLog(fmt.Sprintf("Attempting to connect %s", mc.amqpURI))

		if mc.amqpUser == "" {
			mc.connectionAMQP, err = stomp.Dial("tcp", mc.amqpURI)
		} else {
			mc.connectionAMQP, err = stomp.Dial("tcp", mc.amqpURI,
				stomp.ConnOpt.Login(mc.amqpUser, mc.amqpPass),
				stomp.ConnOpt.HeartBeat(60*time.Second, 60*time.Second),
				stomp.ConnOpt.MsgSendTimeout(10*time.Second),
			)
		}
		if err != nil {
			connectionErrors++
			mc.sendMsgToLog(fmt.Sprintf("Failed to connect %s::%s Retrying...", mc.amqpURI, err.Error()))

			if connectionErrors > 3 {
				mc.sendMsgToLog(fmt.Sprintf("connectionErrors > 3 handleReconnect - Aborting"))
				return
			}
			select {
			case <-mc.shutdown:
				mc.sendMsgToLog(fmt.Sprintf("<-mc.shutdown handleReconnectAMQP"))
				return
			case <-time.After(ReconnectDelay):
			}
			continue
		}

		mc.sendMsgToLog("connected amqp")
		mc.SetConnected(true)
		connectionErrors = 0

		mc.notifyConnClose = make(chan *stomp.Error)

		if done := mc.listenTopicAMQP(); done {
			mc.sendMsgToLog("Im done!")
			break
		}
		mc.SetConnected(false)
		mc.sendMsgToLog(fmt.Sprintf("wait reconnect %s", ReconnectDelay.String()))
		time.Sleep(ReconnectDelay)
	}
}

func (mc *MessageConsumer) listenTopicAMQP() bool {
	for {
		chDelivery, err := mc.initConsumeAMQP()
		if err != nil {
			mc.sendMsgToLog(fmt.Sprintf("Falha ao receber: %s. Retrying...", err.Error()))

			if strings.Contains(err.Error(), "connection closed") {
				mc.sendMsgToLog(fmt.Sprintf("connection closed listenTopicAMQP err"))
				return true
			}

			select {
			case <-mc.shutdown:
				mc.sendMsgToLog(fmt.Sprintf("<-mc.shutdown listenTopicAMQP err"))
				return true
			case <-time.After(ResubscribeDelay):
			}
			continue
		}

		defer mc.subscriptionAMQP.Unsubscribe()

		for {
			data1 := <-chDelivery
			if data1.Err != nil {
				if !strings.Contains(data1.Err.Error(), "message:read timeout") {
					mc.sendMsgToLog(fmt.Sprintf("Error: %s", data1.Err.Error()))
				}
				return false
			}

			mc.sendMsgToLog(fmt.Sprintf("%s", data1.Body))

			mc.chEventMessage <- &EventMessage{
				EventType: mc.amqpDestination,
				EventData: string(data1.Body),
			}
		}
	}
}

func (mc *MessageConsumer) initConsumeAMQP() (<-chan *stomp.Message, error) {
	var err error

	mc.subscriptionAMQP, err = mc.connectionAMQP.Subscribe(mc.amqpDestination, stomp.AckAuto)

	if err != nil {
		return nil, err
	}

	return mc.subscriptionAMQP.C, nil
}

// Shutdown ...
func (mc *MessageConsumer) Shutdown() error {
	mc.shutdown <- struct{}{}

	if err := mc.DisconnectAMQP(); err != nil {
		return err
	}

	close(mc.chEventMessage)

	return nil
}

// DisconnectAMQP ...
func (mc *MessageConsumer) DisconnectAMQP() error {
	if mc.connectionAMQP != nil {
		err := mc.connectionAMQP.Disconnect()
		if err != nil {
			return err
		}
	}
	return nil
}

// IsConnected ...
func (mc *MessageConsumer) IsConnected() bool {
	mc.mux.Lock()
	defer mc.mux.Unlock()

	return mc.connected
}

// SetConnected ...
func (mc *MessageConsumer) SetConnected(value bool) {
	mc.mux.Lock()
	defer mc.mux.Unlock()

	mc.connected = value
}

func (mc *MessageConsumer) sendMsgToLog(msg string) {
	if mc.supressLog {
		return
	}
	//log.Printf("%s\n", msg)
	log.Printf("MC (%s): %s%s%s\n", mc.amqpDestination, Green, msg, Reset)
}
