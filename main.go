package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"crypto/tls"
	"crypto/x509"

	"github.com/Shopify/sarama"
)

var (
	command    string
	hosts      string
	topic      string
	partition  int
	offset     int64
	saslEnable bool
	username   string
	password   string
	tlsEnable  bool
	clientcert string
	clientkey  string
	cacert     string
)

type Consumer struct {
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (consumer *Consumer) Setup(s sarama.ConsumerGroupSession) error {
	log.Printf("consumer setup, memberid: %v", s.MemberID())
	// Mark the consumer as ready
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (consumer *Consumer) Cleanup(s sarama.ConsumerGroupSession) error {
	log.Printf("consumer cleanup, memberid: %v", s.MemberID())

	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (consumer *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {

	for message := range claim.Messages() {
		log.Printf("Consumed message: [%s], offset: [%d]\n", message.Value, message.Offset)
		session.MarkMessage(message, "")

	}

	return nil
}

func main() {
	flag.StringVar(&command, "command", "consumer", "consumer|producer")
	flag.StringVar(&hosts, "host", "localhost:9093", "Common separated kafka hosts")
	flag.StringVar(&topic, "topic", "test--topic", "Kafka topic")
	flag.IntVar(&partition, "partition", 0, "Kafka topic partition")
	flag.Int64Var(&offset, "offset", -1, "offset initial")

	flag.BoolVar(&saslEnable, "sasl", false, "SASL enable")
	flag.StringVar(&username, "username", "", "SASL Username")
	flag.StringVar(&password, "password", "", "SASL Password")

	flag.BoolVar(&tlsEnable, "tls", false, "TLS enable")
	flag.StringVar(&clientcert, "cert", "cert.pem", "Client Certificate")
	flag.StringVar(&clientkey, "key", "key.pem", "Client Key")
	flag.StringVar(&cacert, "ca", "ca.pem", "CA Certificate")
	flag.Parse()

	config := sarama.NewConfig()
	if saslEnable {
		config.Net.SASL.Enable = true
		config.Net.SASL.User = username
		config.Net.SASL.Password = password
	}
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin

	if tlsEnable {
		//sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)
		tlsConfig, err := genTLSConfig(clientcert, clientkey, cacert)
		if err != nil {
			log.Fatal(err)
		}

		config.Net.TLS.Enable = true
		config.Net.TLS.Config = tlsConfig
	}

	client, err := sarama.NewClient(strings.Split(hosts, ","), config)
	if err != nil {
		log.Fatalf("unable to create kafka client: %q", err)
	}

	if command == "consumer" {
		cg, err := sarama.NewConsumerGroupFromClient("testgroup", client)
		if err != nil {
			log.Fatal(err.Error())
		}

		// consumer, err := sarama.NewConsumerFromClient(client)
		// if err != nil {
		// 	log.Fatal(err)
		// }
		// defer consumer.Close()
		// loopConsumer(consumer, topic, offset)
		defer cg.Close()
		h := Consumer{}
		cg.Consume(context.TODO(), []string{topic}, &h)
	} else {

		producer, err := sarama.NewAsyncProducerFromClient(client)
		if err != nil {
			log.Fatal(err)
		}
		defer producer.Close()

		loopProducer(producer, topic)
	}
}

func genTLSConfig(clientcertfile, clientkeyfile, cacertfile string) (*tls.Config, error) {
	// load client cert
	clientcert, err := tls.LoadX509KeyPair(clientcertfile, clientkeyfile)
	if err != nil {
		return nil, err
	}

	// load ca cert pool
	cacert, err := ioutil.ReadFile(cacertfile)
	if err != nil {
		return nil, err
	}
	cacertpool := x509.NewCertPool()
	cacertpool.AppendCertsFromPEM(cacert)

	// generate tlcconfig
	tlsConfig := tls.Config{}
	tlsConfig.RootCAs = cacertpool
	tlsConfig.Certificates = []tls.Certificate{clientcert}
	tlsConfig.BuildNameToCertificate()
	// tlsConfig.InsecureSkipVerify = true // This can be used on test server if domain does not match cert:
	return &tlsConfig, err
}

func loopProducer(producer sarama.AsyncProducer, topic string) {
	scanner := bufio.NewScanner(os.Stdin)
	fmt.Print("> ")
	for scanner.Scan() {
		text := scanner.Text()
		if text == "" {
		} else if text == "exit" || text == "quit" {
			break
		} else {
			producer.Input() <- &sarama.ProducerMessage{Topic: topic, Key: nil, Value: sarama.StringEncoder(text)}
			log.Printf("Produced message: [%s]\n", text)
		}
		fmt.Print("> ")
	}
}

func loopConsumer(consumer sarama.Consumer, topic string, offset int64) {

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM)
	log.Print("Consume Start here!\n")

	ps, err := consumer.Partitions(topic)
	if err != nil {
		log.Printf("partions topic[%s] error: %s", topic, err.Error())
		return
	}

	for idx := range ps {
		pc, err := consumer.ConsumePartition(topic, ps[idx], offset)
		if err != nil {
			continue
		}
		defer pc.Close()
		go func(idx int) {
			for {
				select {

				case msg := <-pc.Messages():
					log.Printf("Consumed message: [%s] from partition %d, offset: [%d]\n", msg.Value, ps[idx], msg.Offset)
				default:
					time.Sleep(time.Second)
				}
			}
		}(idx)

	}

	for {
		select {
		case <-signals:
			log.Printf("Consumer End!\n")
			return
		}
	}
}
