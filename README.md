# stomp-forwarder

Forwards all messages from Apache ActiveMQ server to another, using Stomp protocol. A topic or queue can be chosen independently for reading or writing.

Useful when you have network restrictions for IP whitelisting, especially when working remote and changing locations every week.

## Installation

```bash
go get github.com/luismascotto/stomp-forwarder
```

## Configuration

Create a `config.json` file in the same directory as the executable (see `config.example.json` for reference):

```json
{
    "fromAmqpFullUri": "stomp://admin:password@source.server.com:61613",
    "toAmqpFullUri": "stomp://admin:password@127.0.0.1:61613",
    "fromDestination": "/topic/sourceMessages",
    "toDestination": "/queue/destination",
    "supressLogRepeater": false,
    "supressLogSender": false,
    "supressLogReceiver": false,
    "startAsync": false
}
```

## Usage

```go
import "github.com/luismascotto/stomp-forwarder"

func main() {
    forwarder.StompForwarder()
}
```

## Building

```bash
go build
```

## License

MIT License - see [LICENSE](LICENSE) file for details.
