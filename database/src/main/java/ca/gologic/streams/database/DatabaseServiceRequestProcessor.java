package ca.gologic.streams.database;

import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ca.gologic.streams.database.utils.DatabaseUtils;
import ca.gologic.streams.schema.ServiceEvent;
import ca.gologic.streams.schema.utils.SchemaUtils;
import ca.gologic.streams.schema.utils.StreamProcessorThread;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

public class DatabaseServiceRequestProcessor implements StreamProcessorThread {
	private static Logger LOG = LoggerFactory.getLogger(DatabaseServiceRequestProcessor.class);

	public static final String TOPIC = "devops-service-request";
	public static final String TOPIC_DEST = "devops-service-response";
	public static final String GROUP_ID = "devops-streams-database-processor";

	private boolean running = true;

	@Override
	public void run() {
		final Properties consumerProps = new Properties();

		consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, DatabaseUtils.BOOTSTRAP_SERVERS_CONFIG);
		consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
		consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
		consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		consumerProps.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, DatabaseUtils.SCHEMA_REGISTRY_URL_CONFIG);
		consumerProps.put(AbstractKafkaAvroSerDeConfig.AUTO_REGISTER_SCHEMAS, true);
		consumerProps.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);

		final Properties producerProps = new Properties();
		producerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, DatabaseUtils.BOOTSTRAP_SERVERS_CONFIG);
		producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
		producerProps.put(ProducerConfig.RETRIES_CONFIG, 0);
		producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
		producerProps.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, DatabaseUtils.SCHEMA_REGISTRY_URL_CONFIG);

		try (final KafkaConsumer<String, ServiceEvent> consumer = new KafkaConsumer<>(consumerProps)) {
			KafkaProducer<String, ServiceEvent> producer = new KafkaProducer<String, ServiceEvent>(producerProps);

			consumer.subscribe(Collections.singletonList(TOPIC));
			while (running) {
				@SuppressWarnings("deprecation")
				final ConsumerRecords<String, ServiceEvent> records = consumer.poll(1000);
				for (final ConsumerRecord<String, ServiceEvent> consumerRecord : records) {
					final String eventUUID = consumerRecord.key();
					final ServiceEvent serviceEvent = consumerRecord.value();

					if (serviceEvent.getSubject().toString().equalsIgnoreCase("database")) {
						LOG.info("Processing : " + serviceEvent);
						serviceEvent.setAction(serviceEvent.getAction().toString() + " completed");

						SchemaUtils.sleep();

						final ProducerRecord<String, ServiceEvent> record = new ProducerRecord<String, ServiceEvent>(TOPIC_DEST, eventUUID, serviceEvent);
						producer.send(record);
						producer.flush();
						LOG.info("Processed : " + serviceEvent);
					}
				}
			}
			producer.close();
			consumer.close();
			LOG.info("closed");
		}
	}

	public void stop() {
		running = false;
	}
}
