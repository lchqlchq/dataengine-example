package com.h3c.sparkstreaming.kafka.example;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class JavaSparStreamingKafkaExample {
    private static String brokers = null;
    private static String topic = null;

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.out.println("Please input params: ${bootstrap.servers} ${topic}");
            System.exit(0);
        } else {
            brokers = args[0];
            topic = args[1];
        }
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", brokers);
        kafkaParams.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("group.id", "use_a_separate_group_id_for_each_stream");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);
        //kerberos 环境下，需额外添加如下三项配置，非kerberos环境下去除
        kafkaParams.put("security.protocol", "SASL_PLAINTEXT");
        kafkaParams.put("sasl.mechanism", "GSSAPI");
        kafkaParams.put("sasl.kerberos.service.name", "kafka");
        Collection<String> topics = Arrays.asList(topic);
        SparkConf sparkConf = new SparkConf().setAppName("javasparkstreamingkafkademo");
        JavaStreamingContext javaStreamingContext = new JavaStreamingContext(sparkConf,
                Durations.seconds(1));
        JavaInputDStream<ConsumerRecord<String, String>> dStream = KafkaUtils.createDirectStream(javaStreamingContext,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams));
        dStream.foreachRDD(rdd -> {
            OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
            rdd.foreachPartition(consumerRecords -> {
                OffsetRange o = offsetRanges[TaskContext.get().partitionId()];
        //System.out.println(o.topic() + " " + o.partition() + " " + o.fromOffset() " + o.untilOffset());
                while (consumerRecords.hasNext()) {
                    String value = consumerRecords.next().value();
                    System.out.println(value);
                }
            });
            ((CanCommitOffsets) dStream.inputDStream()).commitAsync(offsetRanges);
        });
        javaStreamingContext.start();
        javaStreamingContext.awaitTermination();
    }
}
