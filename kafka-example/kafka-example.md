# H3C DataEngine Kafka example使用步骤
1. 编译mvn clean package -DskipTests
2. 拷贝kafka-example至客户端或集群内节点,并进入kafka-example目录下
3. 如果集群未开启kerberos，忽略本步；开启kerberos，需要先进行认证(keytab文件从集群获取具体用户认证文件)：

    ``kinit -kt src/main/resources/hadoop.keytab hadoop``

4. 执行kafka生产示例程序：
   
   `` java  -cp target/kafka-example-1.0-SNAPSHOT-jar-with-dependencies.jar com.h3c.kafka.kerberos.example.KafkaProducerDemo management0:6667,management1:6667,managerment2.hde.com:6667 test``

5. 执行kafka消费程序： 

   ``java  -cp target/kafka-example-1.0-SNAPSHOT-jar-with-dependencies.jar com.h3c.kafka.kerberos.example.KafkaConsumerDemo management0:6667,management1:6667,managerment2.hde.com:6667 test``
