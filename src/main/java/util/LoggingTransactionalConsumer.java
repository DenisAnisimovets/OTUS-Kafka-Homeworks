package util;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Map;
import java.util.UUID;

public class LoggingTransactionalConsumer extends AbstractConsumer {
    public LoggingTransactionalConsumer(String name, String topic, Map<String, Object> config, boolean readCommitted) {
        super(name, topic, config);

        this.config.put(ConsumerConfig.GROUP_ID_CONFIG, name + "-" + UUID.randomUUID());
        this.config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        this.config.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

        this.thread.start();
    }

    @Override
    protected void processOne(ConsumerRecord<String, String> record) {
        Utils.log.info("Receive in {}, {} :{} at {}", record.topic(), record.key(), record.value(), record.offset());
    }
}
