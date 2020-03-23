package at.bronzels.libcdcdw;

import org.apache.kafka.clients.producer.*;

import java.util.concurrent.ExecutionException;

public class KafkaProducer210 extends KafkaProducerWNFixedTopic210 {
    private String topic;
    private boolean async;

    public KafkaProducer210(String brokerList, String topic, boolean async) {
        super(brokerList);
        this.topic = getTopicWithPrefix(topic);
        this.async = async;
    }

    public KafkaProducer210(String brokerList, String topic, boolean async, String topicPrefix) {
        super(brokerList, topicPrefix);
        this.topic = getTopicWithPrefix(topic);
        this.async = async;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = getTopicWithPrefix(topic);
    }

    public void send(String value) throws ExecutionException, InterruptedException {
        if (async)
            sendAsync(value);
        else
            sendSync(value);

    }

    public void sendByte(byte[] value) throws ExecutionException, InterruptedException {
        if (async)
            sendByteAsync(value);
        else
            sendByteSync(value);

    }

    public void close() {
        if(producer != null) {
            producer.close();
            producer = null;
        }
    }

    /* Produce a record and wait for server to reply. Throw an exception if something goes wrong */
    private void sendSync(String value) throws ExecutionException, InterruptedException {
        ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, value);
        producer.send(record).get();

    }

    /* Produce a record without waiting for server. This includes a callback that will print an error if something goes wrong */
    private void sendAsync(String value) {
        ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, value);
        producer.send(record, new DemoProducerCallback());
    }

    /* Produce a record and wait for server to reply. Throw an exception if something goes wrong */
    private void sendByteSync(byte[] value) throws ExecutionException, InterruptedException {
        ProducerRecord<String, byte[]> record = new ProducerRecord<String, byte[]>(topic, value);
        producerByte.send(record).get();

    }

    /* Produce a record without waiting for server. This includes a callback that will print an error if something goes wrong */
    private void sendByteAsync(byte[] value) {
        ProducerRecord<String, byte[]> record = new ProducerRecord<String, byte[]>(topic, value);
        producerByte.send(record, new DemoProducerCallback());
    }
}