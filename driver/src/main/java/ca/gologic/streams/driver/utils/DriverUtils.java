package ca.gologic.streams.driver.utils;

import java.io.BufferedReader;
import java.io.Console;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DriverUtils {

	private static Logger LOG = LoggerFactory.getLogger(DriverUtils.class);

	public static String BOOTSTRAP_SERVERS_CONFIG = "ocean.gologic.ca:29092";
	public static String SCHEMA_REGISTRY_URL_CONFIG = "http://ocean.gologic.ca:8081";
	public static final String GROUP_ID = "devops-streams-driver-ui";
	public static final String TOPIC = "devops-service-request";
	public static final String TOPIC_DEST = "devops-service-response";

	private static BufferedReader systemInBfr = new BufferedReader(new InputStreamReader(System.in));

	public static String readInput(String message) throws IOException {
		Console cons = System.console();
		String line = null;

		if (cons != null) {
			line = cons.readLine("%s", message);
		} else {
			System.out.println(message);
			line = systemInBfr.readLine();
		}
		return line;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static void tryResetOffset(Consumer consumer, String topic) {
		for (int i = 1; i <= 10; i++) {
			try {
				Map<TopicPartition, OffsetAndMetadata> commitMessage = new HashMap<>();
				commitMessage.put(new TopicPartition(topic, 0), new OffsetAndMetadata(0));
				consumer.commitSync(commitMessage, Duration.ofSeconds(1));
			} catch (Exception e) {
				LOG.trace("Retries " + i + "/10");
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e1) {
					e1.printStackTrace();
				}
			}
		}
	}
}
