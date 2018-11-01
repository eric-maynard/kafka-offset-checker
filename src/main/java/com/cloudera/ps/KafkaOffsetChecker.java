package com.cloudera.ps;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.text.ParseException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class KafkaOffsetChecker {

    private Properties properties;
    public KafkaOffsetChecker(String bootstrapServers) {
        properties = new Properties();
        properties.put("bootstrap.servers", bootstrapServers);
        properties.put("group.id", "cloudera-kafka-offset-checker");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    }

    public static void main(String[] args) {
        if(args.length < 2) {
            System.err.println("usage: KafkaOffsetChecker bootstrap:server timestamp [topic]");
            System.exit(1);
        } else {
            String bootstrapServers = args[0];
            long timestamp = 0L;
            try {
                timestamp = Long.parseLong(args[1]);
            } catch (NumberFormatException e) {
                System.err.println("Unable to parse " + args[1] + " as a long!");
                System.exit(2);
            }

            String topicRegex = ".*";
            if(args.length > 2) {
                topicRegex = args[2];
            }

            KafkaOffsetChecker kafkaOffsetChecker = new KafkaOffsetChecker(bootstrapServers);

            System.out.println("Getting partition information for pattern:\t" + topicRegex);
            List<TopicPartition> partitions = kafkaOffsetChecker.getPartitions(topicRegex);

            System.out.println("Finding offsets for " + partitions.size() + " topics");
            Map<TopicPartition, OffsetAndTimestamp> offsets = kafkaOffsetChecker.getOffsets(partitions, timestamp);

            System.out.println("Earliest offsets after " + timestamp + ":");
            for(Map.Entry<TopicPartition, OffsetAndTimestamp> entry : offsets.entrySet()) {
                String topic = entry.getKey().topic();
                String partition = String.valueOf(entry.getKey().partition());
                String offset = "null";
                if(entry.getValue() != null) {
                    offset = String.valueOf(entry.getValue().offset());
                }
                System.out.println(topic + "-" + partition + ":\t" + offset);
            }
        }
    }

    private List<TopicPartition> getPartitions(String regex) {
        Pattern pattern = Pattern.compile(regex);
        List<TopicPartition> result = new LinkedList<TopicPartition>();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        try {
            Map<String, List<PartitionInfo>> partitions = consumer.listTopics();
            for(Map.Entry<String, List<PartitionInfo>> topic : partitions.entrySet()) {
                Matcher m = pattern.matcher(topic.getKey());
                if (m.find()) {
                    for(PartitionInfo partition : topic.getValue()) {
                        result.add(new TopicPartition(topic.getKey(), partition.partition()));
                    }
                }
            }
        } finally {
            consumer.close();
        }
        return result;
    }

    private Map<TopicPartition, OffsetAndTimestamp> getOffsets(List<TopicPartition> partitionList, long timestamp) {
        Map<TopicPartition, Long> input = new HashMap<TopicPartition, Long>();
        for(TopicPartition topicPartition : partitionList) {
            input.put(topicPartition, timestamp);
        }

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        Map<TopicPartition, OffsetAndTimestamp> result = new HashMap<TopicPartition, OffsetAndTimestamp>();
        try {
            result = consumer.offsetsForTimes(input);
        } finally {
            consumer.close();
        }

        return result;
    }
}