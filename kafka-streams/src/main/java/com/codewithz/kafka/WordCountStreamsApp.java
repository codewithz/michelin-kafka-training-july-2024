package com.codewithz.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Properties;

public class WordCountStreamsApp {

    public static void main(String[] args) throws Exception{
        final String BOOTSTRAP_SERVER = "localhost:9092";
        final String INPUT_TOPIC = "wordcount.input";
        final String OUTPUT_TOPIC = "wordcount.output";

        Properties properties=new Properties();
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG,"word-count-streams-app");
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,BOOTSTRAP_SERVER);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,Serdes.String().getClass());
        StreamsBuilder builder = new StreamsBuilder();
//        Streams from Kafka

        KStream<String,String> wordCountInput=builder.stream(INPUT_TOPIC);

        KTable<String,Long> wordCount =wordCountInput
                //Maps the text to lower case
                .mapValues(textLine -> textLine.toLowerCase())
                //Split by Space
                .flatMapValues(lowerCasedTextLine -> Arrays.asList(lowerCasedTextLine.split(" ")))
                //Select Key --> to apply a key
                .selectKey((nullKey,word)->word)
//               //Group by Key
                .groupByKey()
//                Count the occurance
                .count();

            wordCount.toStream()
                    .to(OUTPUT_TOPIC, Produced.with(Serdes.String(),Serdes.Long()));

            KafkaStreams streams = new KafkaStreams(builder.build(),properties);

            streams.start();

//            Print the Topology
        System.out.println(streams.toString());

//        Gracefully Shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }
}
