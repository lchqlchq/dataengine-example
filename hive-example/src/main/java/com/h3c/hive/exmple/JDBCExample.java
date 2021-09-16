package com.h3c.hive.exmple;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.*;

/**
 * JDBCExample
 *
 * @author
 * @since 8.0.0
 */
public class JDBCExample {
    private static final Logger logger = LoggerFactory.getLogger(JDBCExample.class);
    private static final String HIVE_DRIVER = "org.apache.hive.jdbc.HiveDriver";


    private static Configuration CONF = null;

    private static final String USERNAME = "";

    private static final String PRNCIPAL_NAME = "hadoop";

    private static final String PATH_TO_KEYTAB = JDBCExample.class.getClassLoader()
            .getResource("hadoop.keytab")
            .getPath();

    private static final String PATH_TO_KRB5_CONF = JDBCExample.class.getClassLoader()
            .getResource("krb5.conf")
            .getPath();

    /* zookeeper节点ip和端口列表 */
    private static String zkQuorum = null;
    private static String zooKeeperNamespace = null;
    private static String serviceDiscoveryMode = "zookeeper";
    private static String auth = null;

    public JDBCExample(){}


    private static void init() throws IOException {
        CONF = new Configuration();
        CONF.addResource(new Path(JDBCExample.class.getClassLoader().getResource("hive-site.xml").getPath()));
        /**
         * zkQuorum获取后的格式为"xxx.xxx.xxx.xxx:2181,xxx.xxx.xxx.xxx:2181,xxx.xxx.xxx.xxx:2181";
         */
        zkQuorum = CONF.get("hive.zookeeper.quorum");
        auth = CONF.get("hive.server2.authentication");
        zooKeeperNamespace = CONF.get("hive.server2.zookeeper.namespace");
    }

    /**
     * 本示例演示了如何使用Hive JDBC接口来执行HQL命令<br>
     * <br>
     *
     * @throws ClassNotFoundException
     * @throws SQLException
     * @throws IOException
     */
    public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException {
        // 参数初始化
        init();

        // 定义HQL，HQL为单条语句，不能包含“;”
        String[] sqls = {
            "CREATE TABLE IF NOT EXISTS employees_info(id INT,name STRING)",
            "INSERT INTO employees_info values (111, 'aaa'),(122, 'asdf')",
            "SELECT COUNT(*) FROM employees_info",
            "DROP TABLE employees_info"
        };

        // 拼接JDBC URL
        StringBuilder strBuilder = new StringBuilder("jdbc:hive2://").append(zkQuorum).append("/");
        strBuilder
                .append(";serviceDiscoveryMode=")
                .append(serviceDiscoveryMode)
                .append(";zooKeeperNamespace=")
                .append(zooKeeperNamespace);
        if ("KERBEROS".equalsIgnoreCase(auth)) {
            CONF.set("hadoop.security.authentication", "Kerberos");
            System.setProperty("java.security.krb5.conf", PATH_TO_KRB5_CONF);
            UserGroupInformation.setConfiguration(CONF);
            UserGroupInformation.loginUserFromKeytab(PRNCIPAL_NAME, PATH_TO_KEYTAB);
        }
        String url = strBuilder.toString();

        // 加载Hive JDBC驱动
        Class.forName(HIVE_DRIVER);

        Connection connection = null;
        try {
            // 获取JDBC连接
            // 如果使用的是普通模式，那么第二个参数需要填写正确的用户名，否则会以匿名用户(anonymous)登录
            connection = DriverManager.getConnection(url, USERNAME, "");

            // 建表
            // 表建完之后，如果要往表中导数据，可以使用LOAD语句将数据导入表中，比如从HDFS上将数据导入表:
            // load data inpath '/tmp/employees.txt' overwrite into table employees_info;
            execDDL(connection, sqls[0]);
            logger.info("Create table success!");

            //插入數據
            execDDL(connection, sqls[1]);
            // 查询
            execDML(connection, sqls[2]);

            // 删表
            execDDL(connection, sqls[3]);
            logger.info("Delete table success!");
        } catch (SQLException e) {
            logger.error("Create connection failed : " + e.getMessage());
        } finally {
            // 关闭JDBC连接
            if (null != connection) {
                connection.close();
            }
        }
    }

    /**
     * Execute DDL Task process
     */
    public static void execDDL(Connection connection, String sql) throws SQLException {
        PreparedStatement statement = null;
        try {
            statement = connection.prepareStatement(sql);
            statement.execute();
        } finally {
            if (null != statement) {
                statement.close();
            }
        }
    }

    /**
     * Execute DML Task process
     */
    public static void execDML(Connection connection, String sql) throws SQLException {
        PreparedStatement statement = null;
        ResultSet resultSet = null;
        ResultSetMetaData resultMetaData = null;

        try {
            // 执行HQL
            statement = connection.prepareStatement(sql);
            resultSet = statement.executeQuery();

            // 输出查询的列名到控制台
            resultMetaData = resultSet.getMetaData();
            int columnCount = resultMetaData.getColumnCount();
            String resultMsg = "";
            for (int i = 1; i <= columnCount; i++) {
                resultMsg += resultMetaData.getColumnLabel(i) + '\t';
            }
            logger.info(resultMsg);

            // 输出查询结果到控制台
            while (resultSet.next()) {
                String result = "";
                for (int i = 1; i <= columnCount; i++) {
                    result += resultSet.getString(i) + '\t';
                }
                logger.info(result);
            }
        } finally {
            if (null != resultSet) {
                resultSet.close();
            }

            if (null != statement) {
                statement.close();
            }
        }
    }
}
