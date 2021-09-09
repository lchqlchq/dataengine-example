# H3C DataEngine Spark example使用步骤
### FemaleInfoCollection
1. 编译mvn clean package -DskipTests
2. 拷贝spark-example至客户端或集群内节点,并进入spark-example目录下
3. 如果集群未开启kerberos，忽略本步；开启kerberos，需要先进行认证(keytab文件从集群获取具体用户认证文件)：

    ``kinit -kt ./conf/hadoop.keytab hadoop``

4. 上传测试文件：hdfs dfs -put ./data /tmp/
5. 提交spark作业： 

   java:``spark-submit --master yarn --class com.h3c.spark.example.JavaFemaleInfoCollection target/spark-examples-1.0-SNAPSHOT.jar /tmp/data/log1.txt  /tmp/female1``

   scala:``spark-submit --master yarn --class com.h3c.spark.example.ScalaFemaleInfoCollection target/spark-examples-1.0-SNAPSHOT.jar /tmp/data/log1.txt  /tmp/female1``

   python: ``spark-submit --master yarn src/main/python/collectFemaleInfo.py /tmp/data/log1.txt /tmp/female2FemaleInfoCollection$``

   SparkSQL: ``spark-submit --master yarn --class com.h3c.spark.example.FemaleInfoCollectionSparkSQL target/spark-examples-1.0-SNAPSHOT.jar /tmp/data/log1.txt``
   
   SparkSQL: 执行spark-sql通过SQL查看表： SELECT * FROM FemaleInfo
   
### HUDI
1. 编译mvn clean package -DskipTests
2. 拷贝spark-example至客户端或集群内节点,并进入spark-example目录下
3. 如果集群未开启kerberos，忽略本步；开启kerberos，需要先进行认证(keytab文件从集群获取具体用户认证文件)：

   ``kinit -kt ./conf/hadoop.keytab hadoop``
4. 确保已安装hudi，提交作业：
   
   ``spark-submit --master yarn --jars /usr/hdp/3.0.1.0-187/hudi/spark/hudi-spark-bundle_2.11-0.6.0.jar,/usr/hdp/3.0.1.0-187/spark2/jars/spark-avro_2.11-2.4.0-cdh6.2.0.jar --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' --class com.h3c.spark.hudi.example.SparkHoodieExample target/spark-examples-1.0-SNAPSHOT.jar``

   默认会在HDFS的/tmp/hudi_cow_table目录下生成文件。
   
   或者使用HoodieDataSourceExample自定义数据存放位置：

   ``spark-submit --master yarn --jars /usr/hdp/3.0.1.0-187/hudi/spark/hudi-spark-bundle_2.11-0.6.0.jar,/usr/hdp/3.0.1.0-187/spark2/jars/spark-avro_2.11-2.4.0-cdh6.2.0.jar --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' --class com.h3c.spark.hudi.example.HoodieDataSourceExample target/spark-examples-1.0-SNAPSHOT.jar  /tmp/hudi hudi``