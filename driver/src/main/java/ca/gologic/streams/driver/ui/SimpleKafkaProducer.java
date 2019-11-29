package ca.gologic.streams.driver.ui;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import ca.gologic.streams.driver.utils.DriverUtils;
import ca.gologic.streams.schema.ServiceEvent;
import ca.gologic.streams.schema.ServiceEventVO;
import ca.gologic.streams.schema.utils.SchemaUtils;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

@Component
public class SimpleKafkaProducer {
  private static Logger LOG = LoggerFactory.getLogger(SimpleKafkaProducer.class);
  
  public static ServiceEventVO latestServiceEvent;
  
  private static ReentrantLock lock  = new ReentrantLock();

  public void write() throws Exception {
    LOG.info("Produce message");

    final Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, DriverUtils.BOOTSTRAP_SERVERS_CONFIG);
    props.put(ProducerConfig.ACKS_CONFIG, "all");
    props.put(ProducerConfig.RETRIES_CONFIG, 0);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
    props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, DriverUtils.SCHEMA_REGISTRY_URL_CONFIG);

    try (KafkaProducer<String, ServiceEvent> producer = new KafkaProducer<String, ServiceEvent>(props)) {

		lock.lock();
		
		String eventUUID = UUID.randomUUID().toString();
		
		String nextEnv = "dev";
		
		String nextMajorVersion = "1";
		String nextProductionVersion = nextMajorVersion + ".0";
		String nextVersion =  nextMajorVersion + ".0-snapshot";
		
		if( latestServiceEvent != null){
			String currentProductionVersion = SchemaUtils.getNextVersion( latestServiceEvent.getVersion(), "production" );
			nextMajorVersion =  SchemaUtils.getNextMajorVersionString(currentProductionVersion) ;
			nextProductionVersion = nextMajorVersion + ".0";
			nextEnv = SchemaUtils.getNextEnv( latestServiceEvent.getEnv() );
			
			if( nextEnv == "dev"){
				nextVersion = nextProductionVersion + "-snapshot";
			}else{
				nextVersion = SchemaUtils.getNextVersion( latestServiceEvent.getVersion(), nextEnv );
			}
		}		
		
		
		ServiceEvent service = ServiceEvent.newBuilder()
		.setSubject("database")
		.setService("mysql")
		.setAction("set")
		.setVersion( nextVersion )
		.setEnv( nextEnv )
		.setProducer("database-team")
		.setStory("STORY-" + nextMajorVersion )
		.setRootEventId(eventUUID)
		.build();
		
		
        final ProducerRecord<String, ServiceEvent> record = new ProducerRecord<String, ServiceEvent>(DriverUtils.TOPIC, eventUUID, service);
        producer.send(record);
        producer.flush();
    	
		lock.unlock();

    } catch (final SerializationException e) {
      LOG.error("FAILED !", e);
      throw e;
    } 
  }
  


}
