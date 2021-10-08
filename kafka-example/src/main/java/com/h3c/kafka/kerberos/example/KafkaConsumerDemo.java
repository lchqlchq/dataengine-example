package com.h3c.kafka.kerberos.example;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.File;
import java.util.Arrays;
import java.util.Properties;

public class KafkaConsumerDemo {
    private static String brokers = null;
    private static String topic = null;

    private static String filePath = System.getProperty("user.dir") + File.separator+ "src" + File.separator + "main" + File.separator + "resources" + File.separator;

    public static void main(String[] args) {
        if (args.length != 2) {
            System.out.println("Usage: java -cp ${jar} kafkaConsumerDemo ${bootstrap.servers} ${topic}");
            System.exit(0);
        } else {
            brokers = args[0];
            topic = args[1];
        }
        //kerberos 环境下，需额外加载如下两项配置文件
        System.setProperty("java.security.auth.login.config", filePath +"kafka_client_jaas.conf");
        System.setProperty("java.security.krb5.conf", filePath + "krb5.conf");
        Properties props = new Properties();
        props.put("bootstrap.servers", brokers);
        props.put("group.id", "kafkaConsumerDemo1");
        props.put("enable.auto.commit", "true");
        props.put("auto.offset.reset", "earliest");
        //props.put("auto.offset.reset", "latest");
        props.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        //kerberos 环境下，需额外添加如下三项配置
        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("sasl.mechanism", "GSSAPI");
        props.put("sasl.kerberos.service.name", "kafka");
        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topic));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(1000);
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record);
            }
            System.out.println("---");
        }
    }
}
