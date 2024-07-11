package com.codewithz;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;

public class WikimediaChangesProducer {
    public static void main(String[] args) throws InterruptedException {
        
        String bootstrapServer="127.0.0.1:9092";
        String topic="wikimedia.changes";

        Logger logger=LoggerFactory
        .getLogger(WikimediaChangesProducer.class);

        // Create Producer Properties
        Properties properties=new Properties();
        properties.
        setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
         bootstrapServer);
        properties.
        setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
         StringSerializer.class.getName());
        properties.
        setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
         StringSerializer.class.getName());

        // For Safe Producer Config 

        properties.
        setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.
        setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.
        setProperty(ProducerConfig.RETRIES_CONFIG,Integer.toString(Integer.MAX_VALUE));

        // High Throughput Producer 
        // (At expense of latency and CPU Usage)

        properties
        .setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties
        .setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties
        .setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024));


        // Create the Producer 

        KafkaProducer<String,String> producer
        =new KafkaProducer<>(properties);

        String url="https://stream.wikimedia.org/v2/stream/recentchange";

        EventHandler eventHandler=new WikimediaChangeHandler(producer, topic);
        // EventHandler eventHandler=TODO;

        EventSource.Builder builder=new 
                            EventSource.Builder(eventHandler, URI.create(url));
        EventSource eventSource=builder.build();

        // Start the producer in a different thread
        eventSource.start();

        // We produce the messages for 10 minutes and stop

        TimeUnit.MINUTES.sleep(10);
    }
}
