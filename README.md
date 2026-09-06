In this project I set up a Kafaka producer: polling the NS API to get live train data in the Netherlands and pass the data down to Kafka which is deployed on my own server.
The response is validated with pydantic, and keyed by train before sending to the topic.
 
In order to reproduce the project, you will need to set up your own simple kafka cluster (server) and pass the server in the .env, and create an account in the ns api portal and add the subscription key in the .evmv.

This repo is the producer part.
The consumer part will be published later.
