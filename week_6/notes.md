## Streaming

### Kafka
1. producers, topics, consumers
2. reliability/robustness => i.e., replication
3. flexibility => topics can be very small/very big
4. scalability
5. integrations like kafka connect, ksql DB

### Microservices
1. consists of multiple small services
2. they communicate with other service using APIs/Message Brokers/Shared DB => these works fine with limited scale/data/APIs
3. with the increase of scale/data we would need some more consistent message pass/streaming service to communicate through
4. one service can post a event in a topic, and another service which is interested/made for those events will listen to that topic and will process/parse the event
5. kafka also offers CDC(Change Data Capture) which is part of kafka connect, it pushes the change as an event to the kafka topic

