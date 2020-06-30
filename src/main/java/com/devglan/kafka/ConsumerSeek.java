package com.devglan.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;

public class ConsumerSeek {

    public static void main(String[] args) {
        if (args.length < 3 ) {
            System.out.println("ConsumeSeek <Kafka Server> <Topic> <UnixTimestmap>");
            return;
        }

        String bootstrapServers = args[0] ;
        String topic = args[1] ;
        long time = Instant.now().minus(10, ChronoUnit.MINUTES).toEpochMilli();
        try {
            time = Long.parseLong(args[2])* 1000;
        } catch ( Exception ex ) {
            System.out.println("invalid time!!!") ;
        }

        int partition = -1;
        if (args.length == 4) {
            partition = Integer.parseInt(args[3]) ;
        }

        Properties properties = new Properties();
        properties.put("bootstrap.servers", bootstrapServers);
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("group.id", "test-group");

        final KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);
        List<String> topics = new ArrayList<String>();
        topics.add(topic);
        kafkaConsumer.subscribe(topics);

        ConsumerRecords<String, String> tmpRecord = kafkaConsumer.poll(10);

        Map<TopicPartition, Long> query = new HashMap<>();

        if ( partition == -1 ) {
            List<PartitionInfo> partitions = kafkaConsumer.partitionsFor(topic);
            for (PartitionInfo p : partitions) {
                query.put(new TopicPartition(topic, p.partition()), time);
            }
        } else {
            query.put(new TopicPartition(topic, partition), time);
            kafkaConsumer.unsubscribe();
            kafkaConsumer.assign(Arrays.asList(new TopicPartition(topic, partition))) ;
        }
        Map<TopicPartition, OffsetAndTimestamp> result = kafkaConsumer.offsetsForTimes(query);


        result.entrySet().stream().forEach(entry-> {
            System.out.printf("partition: %d, offset: %d, timestamp: %d\n", entry.getKey().partition(), entry.getValue().offset(), entry.getValue().timestamp());
            kafkaConsumer.seek(entry.getKey(), entry.getValue().offset());
        }) ;

        try{
            while (true){
                ConsumerRecords<String, String> records = kafkaConsumer.poll(10);
                for (ConsumerRecord<String, String> record: records){
                    System.out.println(String.format("Topic - %s, Partition - %d, Value: %s", record.topic(), record.partition(), record.value()));
                }
            }
        }catch (Exception e){
            System.out.println(e.getMessage());
        }finally {
            kafkaConsumer.close();
        }
    }
}
