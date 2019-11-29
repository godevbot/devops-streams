# devops-streams
Event Driven Continous Delivery powered by Kafka

## Setup Confluent Kafka

* Download https://github.com/confluentinc/examples/blob/5.3.1-post/cp-all-in-one/docker-compose.yml
* Run : docker-compose up -d
* Validate that control center is working : http://localhost:9021

##  Build and run the product

* Run : mvn package 
* Run : java -jar driver/target/streams-driver-1.0.0-SNAPSHOT-spring-boot.jar --broker localhost:9092 --schema-registry http://localhost:8081
* Go to http://localhost:8080 and click "Add next database" button
