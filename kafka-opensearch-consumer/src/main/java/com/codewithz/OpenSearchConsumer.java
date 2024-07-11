package com.codewithz;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.http.HttpHost;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.DoubleDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OpenSearchConsumer {

    public static RestHighLevelClient createOpenSearchClient(){
        String connectionString="http://localhost:9200";

        RestHighLevelClient restHighLevelClient;

        URI url=URI.create(connectionString);
        // extract login info if it exists 
        String userInfo=url.getUserInfo();

        if(userInfo==null){
            // Rest client without security;
            restHighLevelClient=new 
            RestHighLevelClient(RestClient.
            builder(
                new HttpHost(url.getHost(),url.getPort(),"http")
                ));
        }else{
            restHighLevelClient=null;
            // Rest CLient with Security
        }
        return restHighLevelClient;
    }

    public static KafkaConsumer creatKafkaConsumer(){
     final String BOOTSTRAP_SERVER="localhost:9092";
     final String GROUP_ID="opensearch-consumer-group";

     
      Properties properties=new Properties();
        properties.
        setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, 
        BOOTSTRAP_SERVER);
        properties.
        setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, 
        StringDeserializer.class.getName());
        properties.
        setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
         StringDeserializer.class.getName());
        properties.
        setProperty(ConsumerConfig.GROUP_ID_CONFIG,
         GROUP_ID );
        properties.
        setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
        "latest" );
        // earliest/latest/none
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        KafkaConsumer<String,String> consumer=new KafkaConsumer<>(properties);
        return consumer;
    }

    public static void main(String[] args) throws IOException {
        Logger logger=LoggerFactory.getLogger(OpenSearchConsumer.class);
        // Create OpenSearch Client
        RestHighLevelClient openSearchClient=createOpenSearchClient();

        // We need to create an Index if it doesn't index

        try {
        boolean indexExisits=    openSearchClient
            .indices()
                .exists(
                    new GetIndexRequest("wikimedia"),
                     RequestOptions.DEFAULT);
        if(!indexExisits){
            CreateIndexRequest createIndexRequest=
                new CreateIndexRequest("wikimedia");
            openSearchClient
                    .indices()
                    .create(createIndexRequest, 
                    RequestOptions.DEFAULT);

            logger.info("Index wikimedia has been creted");
            
        }else{
            logger.info("Index wikimedia already exists!!");
        }
        } catch (Exception e) {
            // TODO: handle exception
            logger.error("Error in creating Index", e);
        }

        // Create a Kafka Consumer 

        KafkaConsumer<String,String> consumer=creatKafkaConsumer();

        consumer.subscribe(Collections.singleton("wikimedia.changes"));

        // Main Code Logic

        while (true) {
            ConsumerRecords<String,String> records=
            consumer.poll(Duration.ofMillis(3000));

            int recordCount=records.count();
            logger.info("RECIEVED "+recordCount+" record(s)");

            for(ConsumerRecord<String,String> record:records){
                // Send Records to OpenSearch
                // TO make records unique for OpenSearch we will have to assign a key

                String id=record.topic()+"_"+record.partition()+"_"+record.offset();
                try {
                    IndexRequest indexRequest=new IndexRequest("wikimedia")
                                            .source(record.value(),
                                            XContentType.JSON)
                                            .id(id);

                IndexResponse response=openSearchClient
                        .index(indexRequest, RequestOptions.DEFAULT);

                logger.info("Inserted 1 document in OpenSearch with id  :"+response.getId());
                } catch (Exception e) {
                    // TODO: handle exception
                }

                consumer.commitSync();
                logger.info("Offset has been commited");
                
            }
        }

        // Close the client
        // openSearchClient.close();

    }

}
