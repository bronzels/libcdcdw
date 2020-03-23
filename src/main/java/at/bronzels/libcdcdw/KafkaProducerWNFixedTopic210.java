package at.bronzels.libcdcdw;

import at.bronzels.libcdcdw.util.MyString;

import org.apache.kafka.clients.producer.*;

import java.io.Serializable;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaProducerWNFixedTopic210 implements Serializable {
    private String brokerList;

    protected Producer<String, String> producer = null;
    protected Producer<String, byte[]> producerByte = null;

    protected String topicPrefix = null;

    public KafkaProducerWNFixedTopic210(String brokerList) {
        this.brokerList = brokerList;
    }

    public KafkaProducerWNFixedTopic210(String brokerList, String topicPrefix) {
        this.brokerList = brokerList;
        this.topicPrefix = topicPrefix;
    }

    protected String getTopicWithPrefix(String topic) {
        if(topicPrefix != null){
            return MyString.concatBySkippingEmpty(Constants.commonSep, topicPrefix, topic);
        } else return topic;
    }

    private Properties getProps(String destSerializer) {
        Properties ret = new Properties();
        ret.put("bootstrap.servers", brokerList);

        // This is mandatory, even though we don't send keys
        ret.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        ret.put("acks", "1");

        // how many times to retry when produce request fails?
        ret.put("retries", "3");
        ret.put("linger.ms", 5);

        ret.put("value.serializer", destSerializer);

        return ret;
    }

    public void open() {
        if(producer != null)
            return;
        Properties kafkaPropsDefault = new Properties();
        kafkaPropsDefault.put("bootstrap.servers", brokerList);

        // This is mandatory, even though we don't send keys
        kafkaPropsDefault.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaPropsDefault.put("acks", "1");

        // how many times to retry when produce request fails?
        kafkaPropsDefault.put("retries", "3");
        kafkaPropsDefault.put("linger.ms", 5);

        Properties kafkaProps = getProps("org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<String, String>(kafkaProps);

        Properties kafkaPropsByte = getProps("org.apache.kafka.common.serialization.ByteArraySerializer");
        producerByte = new KafkaProducer<String, byte[]>(kafkaPropsByte);
    }

    public void send(String topic, String value, boolean async) throws ExecutionException, InterruptedException {
        if (async)
            sendAsync(getTopicWithPrefix(topic), value);
        else
            sendSync(getTopicWithPrefix(topic), value);

    }

    public void sendByte(String topic, byte[] value, boolean async) throws ExecutionException, InterruptedException {
        if (async)
            sendByteAsync(getTopicWithPrefix(topic), value);
        else
            sendByteSync(getTopicWithPrefix(topic), value);

    }

    public void close() {
        if(producer != null) {
            producer.close();
            producer = null;
        }
    }

    /* Produce a record and wait for server to reply. Throw an exception if something goes wrong */
    private void sendSync(String topic, String value) throws ExecutionException, InterruptedException {
        ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, value);
        producer.send(record).get();

    }

    /* Produce a record without waiting for server. This includes a callback that will print an error if something goes wrong */
    private void sendAsync(String topic, String value) {
        ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, value);
        producer.send(record, new DemoProducerCallback());
    }

    /* Produce a record and wait for server to reply. Throw an exception if something goes wrong */
    private void sendByteSync(String topic, byte[] value) throws ExecutionException, InterruptedException {
        ProducerRecord<String, byte[]> record = new ProducerRecord<String, byte[]>(topic, value);
        producerByte.send(record).get();

    }

    /* Produce a record without waiting for server. This includes a callback that will print an error if something goes wrong */
    private void sendByteAsync(String topic, byte[] value) {
        ProducerRecord<String, byte[]> record = new ProducerRecord<String, byte[]>(topic, value);
        producerByte.send(record, new DemoProducerCallback());
    }

    protected class DemoProducerCallback implements Callback {

        @Override
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            if (e != null) {
                //System.out.println("Error producing to topic " + recordMetadata.topic());
                e.printStackTrace();
            }
        }
    }
}