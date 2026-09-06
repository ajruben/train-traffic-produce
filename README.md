In this project I poll the NS API to get live train data in the Netherlands and pass the data down to Kafka which is deployed on my own server.
The response is validated with pydantic, and keyed by train before sending to the topic.
 
