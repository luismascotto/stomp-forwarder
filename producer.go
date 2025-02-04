package forwarder

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/go-stomp/stomp/v3"
)

type MessageProducer struct {
	amqpURI         string
	amqpUser        string
	amqpPass        string
	amqpDestination string

	connectionAMQP   *stomp.Conn
	subscriptionAMQP *stomp.Subscription

	shutdown chan struct{}

	notifyConnClose chan *stomp.Error

	mux       sync.Mutex
	connected bool //indica se esta conectado no channel

	supressLog bool

	chEventMesssage chan *EventMessage
}

func NewMessageProducer(amqpFullURI, amqpDestination string, supressLog bool) *MessageProducer {
	user, pass, host, err := extractUserPassHost(amqpFullURI)
	if err != nil {
		fmt.Println("Producer: Error extracting user, pass, host from URI")
		return nil
	}

	return newMessageProducer(host, user, pass, amqpDestination, supressLog)
}

func newMessageProducer(amqpURI, amqpUser, amqpPass, amqpDestination string, supressLog bool) *MessageProducer {
	messageProducer := &MessageProducer{
		amqpURI:         amqpURI,
		amqpUser:        amqpUser,
		amqpPass:        amqpPass,
		amqpDestination: amqpDestination,
		chEventMesssage: make(chan *EventMessage, 1023),
		shutdown:        make(chan struct{}),
		supressLog:      supressLog,
	}

	return messageProducer
}

// Start inicia a sessao amqp
func (mp *MessageProducer) Start() {
	mp.handleReconnectAMQP()
}

func (mp *MessageProducer) Send(message *EventMessage) error {
	if mp.chEventMesssage == nil {
		return fmt.Errorf("not initialized")
	}
	mp.chEventMesssage <- message
	return nil
}

// handleReconnect will wait for a connection error on
// notifyConnClose, and then continuously attempt to reconnect.
func (mp *MessageProducer) handleReconnectAMQP() {
	var err error
	var connectionErrors int = 1
	for {
		mp.sendMsgToLog(fmt.Sprintf("Attempting to connect %s", mp.amqpURI))

		if mp.amqpUser == "" {
			mp.connectionAMQP, err = stomp.Dial("tcp", mp.amqpURI)
		} else {
			mp.connectionAMQP, err = stomp.Dial("tcp", mp.amqpURI,
				stomp.ConnOpt.Login(mp.amqpUser, mp.amqpPass),
				stomp.ConnOpt.HeartBeat(60*time.Second, 60*time.Second),
				stomp.ConnOpt.MsgSendTimeout(10*time.Second),
			)
		}
		if err != nil {
			connectionErrors++
			mp.sendMsgToLog(fmt.Sprintf("Failed to connect %s::%s Retrying...", mp.amqpURI, err.Error()))

			if connectionErrors > 3 {
				mp.sendMsgToLog(fmt.Sprintf("connectionErrors > 3 handleReconnect - Aborting"))
				return
			}
			select {
			case <-mp.shutdown:
				mp.sendMsgToLog(fmt.Sprintf("<-mp.shutdown handleReconnectAMQP"))
				return
			case <-time.After(ReconnectDelay):
			}
			continue
		}

		mp.sendMsgToLog("connected amqp")
		mp.SetConnected(true)
		connectionErrors = 0

		mp.notifyConnClose = make(chan *stomp.Error)

		if done := mp.feedTopicAMQP(); done {
			mp.sendMsgToLog("Im done!")
			break
		}
		mp.SetConnected(false)
		mp.sendMsgToLog(fmt.Sprintf("wait reconnect %s", ReconnectDelay.String()))
		time.Sleep(ReconnectDelay)
	}
}

func (mp *MessageProducer) feedTopicAMQP() bool {

	for {
		select {
		case <-mp.shutdown:
			mp.sendMsgToLog(fmt.Sprintf("<-mp.shutdown feedTopicAMQP err"))
			return true
		default:
			msgToSend := <-mp.chEventMesssage
			if len(msgToSend.EventData) == 0 {
				mp.sendMsgToLog("no data to send")
				return false
			}

			err := mp.connectionAMQP.Send(mp.amqpDestination, "text/plain", []byte(msgToSend.EventData))
			if err != nil {
				mp.sendMsgToLog(fmt.Sprintf("Failed to send message %s:%s Retrying...", mp.amqpURI, err.Error()))
				return false
			}
		}
	}
}

// Shutdown ...
func (mp *MessageProducer) Shutdown() error {
	mp.shutdown <- struct{}{}

	if err := mp.DisconnectAMQP(); err != nil {
		return err
	}

	return nil
}

// DisconnectAMQP ...
func (mp *MessageProducer) DisconnectAMQP() error {
	if mp.connectionAMQP != nil {
		err := mp.connectionAMQP.Disconnect()
		if err != nil {
			return err
		}
	}
	return nil
}

// IsConnected ...
func (mp *MessageProducer) IsConnected() bool {
	mp.mux.Lock()
	defer mp.mux.Unlock()

	return mp.connected
}

// SetConnected ...
func (mp *MessageProducer) SetConnected(value bool) {
	mp.mux.Lock()
	defer mp.mux.Unlock()

	mp.connected = value
}

func (mp *MessageProducer) sendMsgToLog(msg string) {
	if mp.supressLog {
		return
	}
	//log.Printf("%s\n", msg)
	log.Printf("MP (%s): %s%s%s\n", mp.amqpDestination, Blue, msg, Reset)
}
