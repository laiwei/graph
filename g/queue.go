package g

import (
	"github.com/nsqio/go-nsq"
	"log"
)

var MQWriter *nsq.Producer

func OpenMQWriter() {
	config := nsq.NewConfig()
	MQWriter, _ = nsq.NewProducer("127.0.0.1:4150", config)

	err := MQWriter.Ping()
	if err != nil {
		log.Fatalf("create mq writer fail:%v", err)
	}
}

func CloseMQWriter() {
	if MQWriter != nil {
		MQWriter.Stop()
	}
}
