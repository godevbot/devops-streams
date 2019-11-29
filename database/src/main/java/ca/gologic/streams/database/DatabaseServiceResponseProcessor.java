package ca.gologic.streams.database;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Reducer;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ca.gologic.streams.database.utils.DatabaseUtils;
import ca.gologic.streams.schema.ServiceEvent;
import ca.gologic.streams.schema.utils.StreamProcessorThread;
import io.confluent.common.utils.TestUtils;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;

public class DatabaseServiceResponseProcessor implements StreamProcessorThread {
	private static Logger LOG = LoggerFactory.getLogger(DatabaseServiceResponseProcessor.class);

	private static final String TOPIC_SRC = "devops-service-response";
	private static final String TOPIC_DEST = "devops-services-audit";
	private static final String STREAM_APP_ID = "devops-services-audit";

	static final Duration INACTIVITY_GAP = Duration.ofMinutes(120);

	private KafkaStreams streams;

	@Override
	public void run() {
		final Properties props = new Properties();

		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, DatabaseUtils.BOOTSTRAP_SERVERS_CONFIG);
		props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, DatabaseUtils.SCHEMA_REGISTRY_URL_CONFIG);

		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, STREAM_APP_ID);
		props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
		props.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getAbsolutePath());

		final StreamsBuilder builder = new StreamsBuilder();
		final KStream<String, ServiceEvent> inputStream = builder.stream(TOPIC_SRC);

		final Map<String, String> serdeConfig = Collections.singletonMap(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
				DatabaseUtils.SCHEMA_REGISTRY_URL_CONFIG);
		final Serde<ServiceEvent> eventSerde = new SpecificAvroSerde<ServiceEvent>();
		eventSerde.configure(serdeConfig, false);

		inputStream.filter((key, value) -> value.getSubject().toString().equalsIgnoreCase("test"))
				.selectKey((key, value) -> value.getEnv() + "/" + value.getRootEventId())
				.groupByKey()
				.windowedBy(SessionWindows.with(INACTIVITY_GAP))
				.reduce(new Reducer<ServiceEvent>() {
					@Override
					public ServiceEvent apply(ServiceEvent aggValue, ServiceEvent newValue) {
						aggValue.setService("aggregated");
						aggValue.setCount(aggValue.getCount() + newValue.getCount());
						return aggValue;
					}
				})
				.toStream()
				.map((key, value) -> new KeyValue<>(
						key.key() + "@" + key.window().start() + "->" + key.window().end(),
						value))
				.to(TOPIC_DEST, Produced.with(Serdes.String(), eventSerde));
		;

		streams = new KafkaStreams(builder.build(), props);
		streams.cleanUp();
		streams.start();
	}

	public void stop() {
		streams.close();
		LOG.info("closed");
	}
}
