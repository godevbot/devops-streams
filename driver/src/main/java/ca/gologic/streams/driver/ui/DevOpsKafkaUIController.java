package ca.gologic.streams.driver.ui;

import java.util.List;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import ca.gologic.streams.driver.utils.DriverUtils;
import ca.gologic.streams.schema.ServiceEventVO;

@RestController
public class DevOpsKafkaUIController {
  private static Logger LOG = LoggerFactory.getLogger(DevOpsKafkaUIController.class);

  private Boolean kafkaConsumerThreadStarted = false;

  private @Autowired SimpleKafkaConsumer consumer;
  private @Autowired SimpleKafkaProducer producer;

  @PostConstruct
  public void initializeConsumers() {
    LOG.info("Initialize consumers");
    initConsumers();
  }

  @GetMapping("/api/requests")
  public List<ServiceEventVO> requests() {
    LOG.debug("Consume topic " + DriverUtils.TOPIC);
    return consumer.getRequests();
  }

  @GetMapping("/api/responses")
  public List<ServiceEventVO> responses() {
    LOG.debug("Consume topic " + DriverUtils.TOPIC_DEST);
    return consumer.getResponses();
  }

  @PostMapping("/api/write")
  public String write() {
    LOG.debug("Produce to topic");
    try {
      producer.write();
    } catch (Exception e) {
      return "error";
    }
    return "ok";
  }

  public void initConsumers() {
    if (kafkaConsumerThreadStarted) {
      LOG.warn("Consumer Already started");
      return;
    }

    Thread kafkaConsumerThreadRequests = new Thread(() -> {
      consumer.consumeRequests();
    });
    Thread kafkaConsumerThreadResponses = new Thread(() -> {
      consumer.consumeResponses();
    });
    kafkaConsumerThreadRequests.start();
    kafkaConsumerThreadResponses.start();
    LOG.info("Consumers Started");

  }


}
