package com.h3c.kafka.kerberos.example;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.File;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class KafkaProducerDemo {
    private static String brokers = null;
    private static String topic = null;

    private static String filePath = System.getProperty("user.dir") + File.separator+ "src" + File.separator + "main" + File.separator + "resources" + File.separator;

    public static void main(String[] args) throws Exception{
        if (args.length != 2) {
            System.out.println("Usage: java -cp ${jar} kafkaProducerDemo ${bootstrap.servers} ${topic}");
            System.exit(0);
        } else {
            brokers = args[0];
            topic = args[1];
        }
        //kerberos 环境下，需额外加载如下两项配置文件
        System.setProperty("java.security.auth.login.config", filePath + "kafka_client_jaas.conf");
        System.setProperty("java.security.krb5.conf", filePath + "krb5.conf");

        Properties props = new Properties();
        props.put("bootstrap.servers", brokers);
        props.put("acks", "1");
        props.put("retries",1);
        props.put("batch.size", 16384);
        props.put("linger.ms", 10);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        //kerberos 环境下，需额外添加如下三项配置
        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("sasl.mechanism", "GSSAPI");
        props.put("sasl.kerberos.service.name", "kafka");
        //kafka topic
        final String sendTopic = topic;
        AdminClient client = KafkaAdminClient.create(props);//创建操作客户端

        //创建topic，指定5个分区
        NewTopic topic = new NewTopic(sendTopic, 5, (short) 2);
        if (!client.listTopics().names().get().contains(sendTopic)) {
            client.createTopics(Arrays.asList(topic));
        }
        client.close();

        final KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        //线程数
        ExecutorService executorService = Executors.newFixedThreadPool(1);
        executorService.execute(new Runnable() {
            @Override
            public void run() {
                while (true){
                    //发送的数据内容
                    String sendData = "timestamp:" + System.currentTimeMillis();
                    producer.send(new ProducerRecord<String, String>(sendTopic, sendData));
                    try{
                        Thread.sleep(1000);
                    }catch (Exception e){
                        e.printStackTrace();
                    }
                    System.out.println( "Topic:" + sendTopic + " Data:" + sendData);
                }
            }
        });
    }
}
