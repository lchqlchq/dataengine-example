## H3C DataEngine example code

The H3C DataEngine example code include HBase, HDFS, Hive, Kafka, Mapreduce, Spark, Storm etc. You can get started in minutes using **Maven**.

### Version Mapping:

Components mapping:

| Component\ version | 5104 |
| --------------------- | --------- |
| Flink                 | 1.12.2    |
| Hive                  | 2.1.1     |
| Tez                   | 0.9.2     |
| Spark                 | 2.4.1 |
| Hudi | 0.6.0 |
| Hadoop                | 3.0.0     |
| HBase                 | 2.1.1     |
| ZooKeeper             | 3.4.5  |
| Oozie                 | 5.1.0     |
| Flume                 | 1.9.0     |
| Kafka                 | 2.3.0     |
| Ranger                | 1.2.0     |
| Storm                 | 1.2.1    |
| Solr                  | 7.4.0     |
| Phoenix               | 5.0.0     |
| Elasticsearch       | 7.4.0    |
| Redis                 | 6.2.1    |

### Quick Links:

- [DataEngine大数据平台产品介绍](http://www.h3c.com/cn/Products___Technology/Products/Big_Data/Catalog/DataEngine/DataEngine/)

### Requirements

To run the examples required:

- Java 1.8+
- Maven 3.0+

#### Specify the Maven Repository

Add the following open source mirror repository address to **mirrors** in the **settings.xml** configuration file.

```
<mirror>
    <id>repo2</id>
    <mirrorOf>central</mirrorOf>
    <url>https://repo1.maven.org/maven2/</url>
</mirror>
```


## Building From Source

Once you check out the code from GitHub, you can build it using maven for every child project, eg:

```
cd hdfs-example
mvn clean install
```

## License

[Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0.html)
