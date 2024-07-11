package com.codewithz;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;

public class WikimediaChangeHandler implements EventHandler {

    Logger logger=LoggerFactory.getLogger(WikimediaChangeHandler.class);
    KafkaProducer<String,String> producer;
    String topic;

    WikimediaChangeHandler(KafkaProducer<String,String> producer,String topic){
        this.producer=producer;
        this.topic=topic;
    }

    @Override
    public void onClosed() throws Exception {
        // Close the producer
        producer.close();
    }

    @Override
    public void onComment(String arg0) throws Exception {
        // DO NOTHING
    }

    @Override
    public void onError(Throwable throwable) {
        // log the error
        logger.error("Error in reading stream", throwable );
    }

    @Override
    public void onMessage(String eventName, MessageEvent message)
     throws Exception {
    //    send the message to kafka topic
        producer.
        send(new ProducerRecord<String,String>(topic,eventName ,
        message.getData()));
    }

    @Override
    public void onOpen() throws Exception {
        // DO NOTHING
    }

}
