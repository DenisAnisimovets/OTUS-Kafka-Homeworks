package com.danis.transactions;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import util.LoggingTransactionalConsumer;
import util.Utils;


public class Transactions {
    public static void main(String[] args) throws Exception {
        Utils.recreateTopics(1, 1, "topic1", "topic2");

        try (
                var producerTransactional = new KafkaProducer<String, String>(Utils.createProducerConfig(b -> {
                    b.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "ex7");
                }));

                LoggingTransactionalConsumer consumerTopic1 = new LoggingTransactionalConsumer("ReadCommittedTopic1", "topic1", Utils.consumerConfig, true);
                LoggingTransactionalConsumer consumerTopic2 = new LoggingTransactionalConsumer("ReadCommittedTopic2", "topic2", Utils.consumerConfig, true)) {
            {

                producerTransactional.initTransactions();

                Utils.log.info("beginTransaction");
                producerTransactional.beginTransaction();
                for (int i = 0; i < 5; i++) {
                    producerTransactional.send(new ProducerRecord<>("topic1", "Transactional topic 1 massage: " + Integer.toString(i)));
                    producerTransactional.send(new ProducerRecord<>("topic2", "Transactional topic 2 massage: " + Integer.toString(i)));
                }
                producerTransactional.commitTransaction();

                producerTransactional.beginTransaction();
                Thread.sleep(500);
                for (int i = 0; i < 2; i++) {
                    producerTransactional.send(new ProducerRecord<>("topic1", "Aborted topic 1 massage: " + Integer.toString(10 * i)));
                    producerTransactional.send(new ProducerRecord<>("topic2", "Aborted topic 2 massage: " + Integer.toString(10 * i)));
                }

                producerTransactional.abortTransaction();
                Thread.sleep(5000);

            }

        }
    }
}
