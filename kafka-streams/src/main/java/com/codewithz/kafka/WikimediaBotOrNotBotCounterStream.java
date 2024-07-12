package com.codewithz.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class WikimediaBotOrNotBotCounterStream {

    public static void main(String[] args) {
        Logger logger= LoggerFactory.getLogger(WikimediaBotOrNotBotCounterStream.class.getName());

        String BOOTSTRAP_SERVERS = "localhost:9092";
        String INPUT_TOPIC = "wikimedia.changes";
        String OUTPUT_TOPIC = "wikimedia.stats.bots";
        String BOT_COUNT_STORE="bot.count.store";

        ObjectMapper objectMapper = new ObjectMapper();

        Properties properties=new Properties();
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG,"wikimedia-bot-count-app");
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,BOOTSTRAP_SERVERS);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,Serdes.String().getClass());

        StreamsBuilder builder=new StreamsBuilder();
        KStream<String,String> wikimediaStream=builder.stream(INPUT_TOPIC);

        wikimediaStream.
                mapValues(json -> {
                    try{
                        JsonNode jsonNode= objectMapper.readTree(json);
                        if(jsonNode.get("bot").asBoolean()){
                            return "bot";
                        }
                            return "non-bot";

                 }
                    catch (IOException e){
                        logger.error(e.getMessage());
                        return "parse-error";
                    }
                })
                .groupBy((key,botOrNonBot)->botOrNonBot)
                .count(Materialized.<String,Long, KeyValueStore<Bytes, byte[]>>as(BOT_COUNT_STORE)
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.Long())
                )
                .toStream()
                .mapValues((key,value)->{
                    Map<String,Long> keyValueMap=new HashMap<String ,Long>(){
                        {
                            logger.info("COUNT",String.valueOf(key)+"---"+value);
                            put(String.valueOf(key),value);
                        }
                    };

                    try{
                        System.out.println(keyValueMap);
                        return objectMapper.writeValueAsString(keyValueMap);
                    }
                    catch (JsonProcessingException e){
                        System.out.println(e.getMessage());
                        return null;
                    }
                })
                .to(OUTPUT_TOPIC);


        KafkaStreams streams=new KafkaStreams(builder.build(),properties);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }
}
