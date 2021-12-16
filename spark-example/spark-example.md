# H3C DataEngine Spark example使用步骤
### 入门：读取文件内容并对性别为Female进行分组排序，FemaleInfoCollection
1. 编译mvn clean package -DskipTests
2. 拷贝spark-example至客户端或集群内节点,并进入spark-example目录下
3. 如果集群未开启kerberos，忽略本步；开启kerberos，需要先进行认证(keytab文件从集群获取具体用户认证文件)：

    ``kinit -kt ./conf/hadoop.keytab hadoop``

4. 上传测试文件：hdfs dfs -put ./data /tmp/
5. 提交spark作业： 

   java:```spark-submit --master yarn --class com.h3c.spark.example.JavaFemaleInfoCollection target/spark-examples-1.0-SNAPSHOT.jar /tmp/data/log1.txt  /tmp/female1```

   scala:```spark-submit --master yarn --class com.h3c.spark.example.ScalaFemaleInfoCollection target/spark-examples-1.0-SNAPSHOT.jar /tmp/data/log1.txt  /tmp/female1```

   python: ```spark-submit --master yarn src/main/python/collectFemaleInfo.py /tmp/data/log1.txt /tmp/female2FemaleInfoCollection$```

   SparkSQL: ```spark-submit --master yarn --class com.h3c.spark.example.FemaleInfoCollectionSparkSQL target/spark-examples-1.0-SNAPSHOT.jar /tmp/data/log1.txt```
   
   SparkSQL: 执行spark-sql通过SQL查看表： SELECT * FROM FemaleInfo
   
### Spark操作HUDI文件：SparkHoodieExample
1. 编译mvn clean package -DskipTests
2. 拷贝spark-example至客户端或集群内节点,并进入spark-example目录下
3. 如果集群未开启kerberos，忽略本步；开启kerberos，需要先进行认证(keytab文件从集群获取具体用户认证文件)：

   ``kinit -kt ./conf/hadoop.keytab hadoop``
4. 确保已安装hudi，提交作业：

   ```
   spark-submit --master yarn --jars /usr/hdp/3.0.1.0-187/hudi/spark/hudi-spark-bundle_2.11-0.6.0.jar,/usr/hdp/3.0.1.0-187/spark2/jars/spark-avro_2.11-2.4.0-cdh6.2.0.jar --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' --class com.h3c.spark.hudi.example.SparkHoodieExample target/spark-examples-1.0-SNAPSHOT.jar
   ```
   
   默认会在HDFS的/tmp/hudi_cow_table目录下生成文件。
   或者使用HoodieDataSourceExample自定义数据存放位置：

   ```
   spark-submit --master yarn --jars /usr/hdp/3.0.1.0-187/hudi/spark/hudi-spark-bundle_2.11-0.6.0.jar,/usr/hdp/3.0.1.0-187/spark2/jars/spark-avro_2.11-2.4.0-cdh6.2.0.jar --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' --class com.h3c.spark.hudi.example.HoodieDataSourceExample target/spark-examples-1.0-SNAPSHOT.jar  /tmp/hudi hudi
   ```

### SparkStreaming读取kafka：JavaSparStreamingKafkaExample
1. 编译mvn clean package -DskipTests
2. 启动kafka生产程序（可参照kafka-example下）
3. 拷贝spark-example至客户端或集群内节点,并进入spark-example目录下(注意执行目录要正确，否则会找不到认证文件)
4. 如果集群未开启kerberos，注意需要修改JavaSparStreamingKafkaExample代码35行，关闭kafka认证参数：

   ```
   //kerberos 环境下，需额外添加如下三项配置，非kerberos环境下去除
   kafkaParams.put("security.protocol", "SASL_PLAINTEXT");
   kafkaParams.put("sasl.mechanism", "GSSAPI");
   kafkaParams.put("sasl.kerberos.service.name", "kafka");
   ```

   重新打包后，执行Spark程序：
   
   ```
   spark-submit  --class com.h3c.sparkstreaming.kafka.example.JavaSparStreamingKafkaExample --master yarn --deploy-mode cluster --executor-memory 1G --num-executors 3 target/spark-example-1.0-SNAPSHOT.jar management0:6667 test2
   ```
   

5. 开启kerberos情况下，需要先在大数据平台中获取认证文件（集群管理->安全管理->用户管理->认证文件下载），并将解压后的keytab文件放在conf目录，然后在Spark程序执行端先进行认证：

   ``
   kinit -kt ./conf/hadoop.keytab hadoop
   ``

   认证通过后，运行Spark提交命令：

   **(1) YARN CLUSTER 模式:**
   
   作业提交示例（业务提交时替换hadoop.keytab认证文件为实际使用到的认证文件,kafka_client_jaas.conf中内容也修改为对应实际用户信息）：

   ```
   spark-submit --files "./conf/kafka_client_jaas.conf,./conf/hadoop.keytab" \
   --driver-java-options "-Djava.security.auth.login.config=./kafka_client_jaas.conf" \
   --conf "spark.executor.extraJavaOptions=-Djava.security.auth.login.config=./kafka_client_jaas.conf"  \
   --class com.h3c.sparkstreaming.kafka.example.JavaSparStreamingKafkaExample \
   --master yarn \
   --deploy-mode cluster \
   --executor-memory 1G \
   --num-executors 1 \
   target/spark-example-1.0-SNAPSHOT.jar management0:6667 test
   ```

   >**参数说明：**
   >- --files : 认证文件上传到分布式缓存，使得远端driver或executor能够加载到，默认加载spark的jaas认证文件和keytab认证文件即可，对应此例中加载文件为当前运行目录下的*./conf/kafka_client_jaas.conf*和*./conf/hadoop.keytab*，具体案例中修改为运行作业的对应目录即可；
   >- --driver-java-options : 指定driver端的运行参数，必须配置*java.security.auth.login.config*运行参数，否则访问kafka会认证失败，参数值指定分布式缓存相对路径 *./kafka_client_jaas.conf* 即可；
   >- --conf： 通过配置指定Spark executor端的JVM参数*spark.executor.extraJavaOptions*，与driver端参数类似，具体值也为*java.security.auth.login.config=./kafka_client_jaas.conf*
   >- --class：指定Spark作业主类，本例中为*com.h3c.sparkstreaming.kafka.example.JavaSparStreamingKafkaExample*
   >- --master：指定Spark作业运行模式，一般指定为yarn模式
   >- --deploy-mode：Spark作业在yarn运行模式下又可以支持cluster和cient两种，其中custer模式Spark作业Driver会运行在yarn的container中，client模式Driver运行在本地
   >- --executor-memory：指定spark作业运行的执行器内存
   >- --num-executors：指定spark作业运行的执行器个数
   >- application jar:应用程序jar包
   >- zookeeepr:node1:2181,node2:2181,node3:2181
   >- kafka topic name:消费的topic名称

   kafka_client_jaas.conf内容如下，*keyTab="./hadoop.keytab"*和*principal="hadoop"*将hadoop用户修改为实际业务用户配置即可： 
   ```
   KafkaClient {
   com.sun.security.auth.module.Krb5LoginModule required
   useKeyTab=true
   keyTab="./hadoop.keytab" 
   storeKey=true
   useTicketCache=false
   serviceName="kafka"
   principal="hadoop";
   };
   ```

   **(2) YARN Client 模式:**

   作业提交示例（业务提交时替换hadoop.keytab认证文件为实际使用到的认证文件,driver_kafka_client_jaas.conf和executor_kafka_client_jaas中内容也修改为对应实际用户信息）：

   ```
   spark-submit --files "./conf/executor_kafka_client_jaas.conf,./conf/hadoop.keytab" \
   --driver-java-options "-Djava.security.auth.login.config=./conf/driver_kafka_client_jaas.conf" \
   --conf "spark.executor.extraJavaOptions=-Djava.security.auth.login.config=./executor_kafka_client_jaas.conf" \
   --class com.h3c.sparkstreaming.kafka.example.JavaSparStreamingKafkaExample \
   --master yarn \
   --deploy-mode client \
   --executor-memory 1G \
   --num-executors 1 \
   target/spark-example-1.0-SNAPSHOT.jar management0:6667 test
   ```
   >**参数说明：**
   >- --files : 认证文件上传到分布式缓存，使得远端executor能够加载到，默认加载spark的jaas认证文件和keytab认证文件即可，对应此例中加载文件为当前运行目录下的*./conf/executor_kafka_client_jaas.conf*和*./conf/hadoop.keytab*，具体案例中修改为运行作业的对应目录即可；
   >- --driver-java-options : 指定driver端的运行参数，必须配置*java.security.auth.login.config*运行参数，否则访问kafka会认证失败，参数值指定本地路径 *./conf/driver_kafka_client_jaas.conf* 即可；
   >- --conf： 通过配置指定Spark executor端的JVM参数*spark.executor.extraJavaOptions*，与driver端参数类似，具体值也为*java.security.auth.login.config=./executor_kafka_client_jaas.conf*
   >- --class：指定Spark作业主类，本例中为*com.h3c.sparkstreaming.kafka.example.JavaSparStreamingKafkaExample*
   >- --master：指定Spark作业运行模式，一般指定为yarn模式
   >- --deploy-mode：Spark作业在yarn运行模式为cient，Driver运行在本地
   >- --executor-memory：指定spark作业运行的执行器内存
   >- --num-executors：指定spark作业运行的执行器个数
   >- application jar:应用程序jar包
   >- zookeeepr:node1:2181,node2:2181,node3:2181
   >- kafka topic name:消费的topic名称

   driver_kafka_client_jaas.conf内容如下，client模式下，driver会在client端启动，*keyTab="./conf/hadoop.keytab"* 为本地keytab存放位置，修改hadoop.key为实际用户keytab即可，同时修改*principal="hadoop"* 将hadoop用户修改为实际业务用户配置：
   ```
   KafkaClient {
   com.sun.security.auth.module.Krb5LoginModule required
   useKeyTab=true
   keyTab="./conf/hadoop.keytab"
   storeKey=true
   useTicketCache=false
   serviceName="kafka"
   principal="hadoop";
   };
   ```

   executor_kafka_client_jaas.conf内容如下，*keyTab="./hadoop.keytab"*和*principal="hadoop"*将hadoop用户修改为实际业务用户配置即可：
   ```
   KafkaClient {
   com.sun.security.auth.module.Krb5LoginModule required
   useKeyTab=true
   keyTab="./hadoop.keytab" 
   storeKey=true
   useTicketCache=false
   serviceName="kafka"
   principal="hadoop";
   };
   ```