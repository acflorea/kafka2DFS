package main

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/colinmarc/hdfs"
	"os"
	"flag"
	"regexp"
	"github.com/rs/xid"
	"log"
	"time"
)

func main() {

	// Parameters
	bootstrapServersPtr := flag.String("bootstrapServers", "localhost:9092", "List of Kafka servers")
	topicPtr := flag.String("topic", "test_topic", "Kafka topic to connect to")

	dfsServerPtr := flag.String("dfsServer", "localhost:9000", "Location of the HDFS NameNode")
	dfsRootFolderPtr := flag.String("dfsRootFolder", "/_test/kafka2dfs", "DFS root folder")

	flag.Parse()

	topic := *topicPtr
	dfsRootFolder := *dfsRootFolderPtr
	bootstrapServers := *bootstrapServersPtr
	dfsServer := * dfsServerPtr

	log.Printf("Start dumping messages from %s into %s", topic, dfsRootFolder)

	// Date retriever
	r := regexp.MustCompile("\"date\": ?\"(.{10})\"")

	// System values
	fileExtension := ".json"
	flushPeriodically := false
	flushFrequency := 10 * time.Second
	lineSeparator := []byte("\n")

	// Initialize Kafka consumer

	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": bootstrapServers,
		"group.id":          "myGroup_005",
		"auto.offset.reset": "earliest",
	})

	if err != nil {
		panic(err)
	}

	log.Println("Consumer initialized! on ", bootstrapServers)

	// Initialize DFS producer

	client, err := hdfs.New(dfsServer)

	if err != nil {
		log.Panicln(err)
		return
	}

	log.Println("DFS client initialized")

	client.MkdirAll(dfsRootFolder, os.ModeDir)

	if err != nil {
		log.Panicln(err)
		return
	}

	// Subscribe (when leaving the program, close it)
	consumer.SubscribeTopics([]string{"myTopic", topic}, nil)
	defer consumer.Close()

	log.Println("Subscription successful")

	// Map of writers (when leaving the program, flush and close them)
	writers := make(map[string]*hdfs.FileWriter)
	defer func() {
		for _, writer := range writers {
			writer.Flush()
			writer.Close()
		}
	}()

	// Keep a hash of keys
	keys := make(map[string]bool)

	processedCounter := 0
	// Log the counter once per second
	go func(c *int) {
		formerValue := *c
		for {
			time.Sleep(time.Second)
			log.Println(*c - formerValue)
			formerValue = *c
		}
	}(&processedCounter)

	for {
		msg, err := consumer.ReadMessage(-1)
		if err == nil {

			processedCounter ++

			//log.Println(msg.TopicPartition.Partition, msg.TopicPartition.Offset)

			newKey := extractKey(r, msg)
			dirName := dfsRootFolder + "/" + newKey

			if _, ok := keys[newKey]; !ok {
				// make sure the folder with the key name exists
				client.MkdirAll(dirName, os.ModeDir)
				keys[newKey] = true
			}

			if writer, ok := writers[dirName]; ok {
				writer.Write(msg.Value)
				writer.Write(lineSeparator)
			} else {
				fileName := dirName + "/" + xid.New().String() + fileExtension
				newWriter, err := client.Create(fileName)

				if err != nil {
					log.Panicln(err)
					return
				}

				writers[dirName] = newWriter
				newWriter.Write(msg.Value)
				newWriter.Write(lineSeparator)

				// If required, schedule a periodic flush
				if flushPeriodically {
					go func(w *hdfs.FileWriter, interval time.Duration) {
						for {
							// Flush every 60 seconds ??? is this wise ?
							time.Sleep(interval)
							log.Println("Flush!!!")
							w.Flush()
						}
					}(newWriter, flushFrequency)
				}
			}

		} else {
			log.Panicln("Consumer error: %v (%v)\n", err, msg)
			break
		}
	}

}

func extractKey(r *regexp.Regexp, msg *kafka.Message) string {
	// attempt to retrieve the message key
	key := "unknown"
	if match := r.FindStringSubmatch(string(msg.Value)); r.Match(msg.Value) && len(match) > 1 {
		key = match[1]
	}
	return key
}
