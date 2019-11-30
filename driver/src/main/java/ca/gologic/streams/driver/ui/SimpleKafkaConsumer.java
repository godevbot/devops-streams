package ca.gologic.streams.driver.ui;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import ca.gologic.streams.driver.utils.DriverUtils;
import ca.gologic.streams.schema.ServiceEvent;
import ca.gologic.streams.schema.ServiceEventVO;
import ca.gologic.streams.schema.utils.SchemaUtils;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;

@Component
public class SimpleKafkaConsumer {
  private static Logger LOG = LoggerFactory.getLogger(SimpleKafkaConsumer.class);

  private List<ServiceEventVO> requests = new ArrayList<>();
  private List<ServiceEventVO> responses = new ArrayList<>();

  public List<ServiceEventVO> getRequests() {
    return requests;
  }
  public List<ServiceEventVO> getResponses() {
    return responses;
  }

  public void consumeRequests() {
    LOG.info("Run consumeRequest");

    KafkaConsumer<String, ServiceEvent> kafkaConsumer = buildKafkaConsumer();
    
    DriverUtils.tryResetOffset(kafkaConsumer, DriverUtils.TOPIC);

    kafkaConsumer.subscribe(Arrays.asList(DriverUtils.TOPIC));

    consume(kafkaConsumer, requests);
  }

  public void consumeResponses() {
    LOG.info("Run consumeResponse");

    KafkaConsumer<String, ServiceEvent> kafkaConsumer = buildKafkaConsumer();
    
    DriverUtils.tryResetOffset(kafkaConsumer, DriverUtils.TOPIC_DEST);

    kafkaConsumer.subscribe(Arrays.asList(DriverUtils.TOPIC_DEST));

    consume(kafkaConsumer, responses);
  }

  /**
   * @param kafkaConsumer
   */
  private void consume(KafkaConsumer<String, ServiceEvent> kafkaConsumer, List<ServiceEventVO> array) {
    while (true) {
      ConsumerRecords<String, ServiceEvent> records = kafkaConsumer.poll(Duration.ofMillis(100));
      for (ConsumerRecord<String, ServiceEvent> record : records) {
        LOG.info("Received a record: " + record.value());

        ServiceEvent serviceEvent = record.value();
        ServiceEventVO serviceEventVO = SchemaUtils.getServiceEventVO(serviceEvent);

        SimpleKafkaProducer.setLatestVersionServiceEvent(serviceEventVO);
        
        array.add(serviceEventVO);
      }
    }
  }

  private KafkaConsumer<String, ServiceEvent> buildKafkaConsumer() {
    // Defining Kafka consumer properties.
    Properties consumerProperties = new Properties();
    consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, DriverUtils.BOOTSTRAP_SERVERS_CONFIG);
    consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, DriverUtils.GROUP_ID);
    consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
    consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    consumerProperties.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, DriverUtils.SCHEMA_REGISTRY_URL_CONFIG);
    consumerProperties.put(AbstractKafkaAvroSerDeConfig.AUTO_REGISTER_SCHEMAS, true);
    consumerProperties.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
    KafkaConsumer<String, ServiceEvent> kafkaConsumer = new KafkaConsumer<>(consumerProperties);
    return kafkaConsumer;
  }

}
