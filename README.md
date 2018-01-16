[![GoDoc](https://godoc.org/github.com/savaki/kstreams?status.svg)](https://godoc.org/github.com/savaki/kstreams)

kstreams
------

Implementation of [Apache Kafka Streams API](https://kafka.apache.org/documentation/streams/) in Golang.

This repository is under heavy construction and is far far from production ready.

## Motivation

Kafka Streams is a client library for building applications and microservices, 
where the input and output data are stored in Kafka clusters. It combines the 
simplicity of writing and deploying standard Java and Scala applications on the 
client side with the benefits of Kafka's server-side cluster technology.

```kstreams``` is a golang implementation of the Kafka Streams API.  It is intended
to provider go developers access to the concepts and capabilities of the Kafka Streams 
API. 

## Getting Started

### Dependencies

The goal will be to have a 100% pure go implementation of the Kafka Streams API.  All
that will be required is a running Kafka cluster and Go 1.7+

```bash
go get -u github.com/savaki/kstreams
``` 

## Status

under heavy construction
