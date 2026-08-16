# Kafka (Apache)

## Introduction

### Event Streaming

> Event streaming is the digital equivalent of the human body's central nervous system. The technical foundation for the 'Always On' World.

<br>

**Event Streaming**

1. **Capture**: Practice of **Capturing Data** in **real-time** from **event sources** like databases, sensors, applications. This is in the form of **Streams of events**.
2. **Storage**: Storing these event streams durably for later retrieval.
3. **Processing**: manipulate, process and react to event streams in real-time as well as retrospectively.
4. **Routing**: Event streams need to reach different destination technologies as needed.

### The What: Apache Kafka is an event streaming platform

Kafka combines three key capabilities for event streaming end-to-end in one solution:

1. to **publish** (write) and **subscribe to** (read) streams of events. (including continuous import/export of your data from other systems).
2. to **Store** streams of events durably and reliably for as long as you want.
3. to **Process** streams of events as they occur or retrospectively.

These capabilities is provided on the following manner:

- **Distributed** : Independent computers (nodes) work together over a network to solve a larger problem.
- **Highly Scalable**: In distributed systems the ability to handle increasing workload/users/data without degrading performance or reliability. More on this in a sepparate markdown 'scalability'. The two key characteristics for kafka are:
  - 1. **Replication**: Maintaning copies of data across multiple nodes to increase availability, backup and fault tolerance.
  - 2. **Partitioning** (sharding): To divide large datasets into smaller parts distributed accross nodes, to support large-scale data handling and parallel processing.
  - Kafka follows a Microservices architecture, applications are broken down in smaller independent services that communicate through APIs.
- **Elastic**: Ability of the system to automatically add or remove resources like extra servers or containers.
- **Fault-tolerant**
- **Secure**

Kafka can be deployed on bare-metal servers (Not a virtual machine, typically used by one consumer).  
Kafka can be deployed on virtual machines, containers and on-premises and on cloud.

### The How:

Kafka is a distributed system, consisting of servers and clients.  
Communication is via TCP network protocol.

**Event**: Records that "something happens". (Record/message).  
An event has key, value, timestamp and metatdata headers.

**Producer**: Client application that **publish** events to Kafka.
**Consummer**: Client applications that subscribe (read + process) these events.  
They are fully decoupled and don't know about each other.  
<br>
**Topics**: Organisation and durable storage of events (on disk/fs). Topics are always multi-producer and multi-subscriber (0+).  
Events can be read as often as needed, unlike traditional messaging systems - not deleted after consumption.  
Define how long to retain events per topic.  
<br>
**Partitioning**: Topics are partitioned - spread over 'buckets' on different Kafka br okers - Distributed data important for scalability to allow client application to read and write data from/to many brokers at same time. Published events are appeneded to a topic's partition, events with same key to same partition. read in same order as write guaranteed.  
<br>
**Replication**: copy data across brokers - make data fault-tolerant + highly-available. Replication factor of 3 is common production setting, at topic-partititon level.
