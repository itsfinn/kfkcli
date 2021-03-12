# kfkcli
a simple kafka cli with go

# Install

With Go Toolchain 1.16+, run:
```shell
$ go install github.com/itsfinn/kfkcli@latest
```
With Go Toolchain 1.15-, run:
```shell
$ go get -u github.com/itsfinn/kfkcli
```

# Usage

get usage with:
```shell
$ kfkcli -h
Usage of kfkcli:
  -ca string
    	CA Certificate (default "ca.pem")
  -cert string
    	Client Certificate (default "cert.pem")
  -command string
    	consumer|producer (default "consumer")
  -host string
    	Common separated kafka hosts (default "localhost:9093")
  -key string
    	Client Key (default "key.pem")
  -offset int
    	offset initial (default -1)
  -partition int
    	Kafka topic partition
  -password string
    	SASL Password
  -sasl
    	SASL enable
  -tls
    	TLS enable
  -topic string
    	Kafka topic (default "test--topic")
  -username string
    	SASL Username
```