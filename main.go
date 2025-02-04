package forwarder

import (
	"fmt"
	"time"

	"github.com/kardianos/osext"
	"github.com/spf13/viper"
)

type Configuration struct {
	FromAmqpFullUri    string
	ToAmqpFullUri      string
	FromTopicName      string
	ToTopicName        string
	SupressLogRepeater bool
	SupressLogSender   bool
	SupressLogReceiver bool
	StartAsync         bool
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

var Config Configuration

func main() {
	if err := ReadSettings(&Config); err != nil {
		fmt.Println("Error reading settings")
		return
	}

	amqpProxy := NewMessageRepeater(&Config)
	if amqpProxy == nil {
		fmt.Println("Error creating repeater")
		return
	}
	amqpProxy.sendMsgToLog("Starting Repeater...")

	if Config.StartAsync {
		go amqpProxy.Start()

		//amqpProxy.sendMsgToLog("Working...")
		// Wait for a signal to quit:
		<-amqpProxy.exitch
	} else {
		amqpProxy.Start()
	}
}

func ReadSettings(conf *Configuration) error {
	viper.SetConfigName("config")
	exePath, _ := osext.ExecutableFolder()
	viper.AddConfigPath(exePath)
	viper.AddConfigPath(".")
	err := viper.ReadInConfig() // Find and read the config file
	if err != nil {             // Handle errors reading the config file
		return fmt.Errorf("fatal error reading config file: %w", err)
	}
	// viper.OnConfigChange(func(e fsnotify.Event) {
	// 	//fmt.Println("Config file changed:", e.Name)
	// 	_ = viper.Unmarshal(&conf)
	// })
	// viper.WatchConfig()
	err = viper.Unmarshal(&conf)
	if err != nil {
		return fmt.Errorf("fatal error unmarshalling config file: %w", err)
	}
	return nil
}
