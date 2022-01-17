package main

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

const (
	ENV_PREFIX          = "dmpkfk"
	ENV_BOOTSTRAPSERVER = "bootstrapserver"
	ENV_GROUPID         = "groupid"
	ENV_TOPICNAME       = "topicname"
)

func setupViper() {
	log.Info("Bind Environment Parameters")
	viper.SetEnvPrefix(ENV_PREFIX)
	viper.BindEnv(ENV_BOOTSTRAPSERVER)
	viper.BindEnv(ENV_GROUPID)
	viper.BindEnv(ENV_TOPICNAME)
}

func createConsumer() (consumer *kafka.Consumer) {
	log.Info("Create Consumer")
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": viper.GetString(ENV_BOOTSTRAPSERVER),
		"group.id":          viper.GetString(ENV_GROUPID),
		"auto.offset.reset": "earliest",
	})

	if err != nil {
		panic(err)
	}
	return
}

func subscribeToTopic(consumer *kafka.Consumer) {
	log.Info("Subscribe")
	consumer.SubscribeTopics(
		[]string{viper.GetString(ENV_TOPICNAME)},
		nil)

}

func dumpKafkaMessages(consumer *kafka.Consumer) {
	log.Info("Start reading")
	for {
		msg, err := consumer.ReadMessage(-1)
		if err == nil {
			log.WithFields(log.Fields{"partition": msg.TopicPartition, "message": string(msg.Value)}).Info("Message received")
		} else {
			// The client will automatically try to recover from all errors.
			log.WithFields(log.Fields{"error": err, "message": msg}).Error("Consumer Error")
		}
	}
}

func main() {
	log.Info("Starting")
	setupViper()

	consumer := createConsumer()
	subscribeToTopic(consumer)

	defer consumer.Close()
	dumpKafkaMessages(consumer)
}
