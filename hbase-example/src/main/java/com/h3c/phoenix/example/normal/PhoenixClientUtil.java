package com.h3c.phoenix.example.normal;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

public class PhoenixClientUtil {
    private static final Log LOG = LogFactory.getLog(PhoenixClientUtil.class);
    private static String driver = "org.apache.phoenix.jdbc.PhoenixDriver";
    private static Configuration conf = null;
    private static Properties props = new Properties();

    public static Connection getConnection() throws SQLException {
        if (conf == null) {
            LOG.error("Configuration must be initalized.");
        }
        String zkUrl = conf.get("hbase.zookeeper.quorum");
        String zkParent = conf.get("zookeeper.znode.parent");
        String url = "jdbc:phoenix:" + zkUrl + ":" + zkParent;

        try {
            Class.forName(driver);
            return DriverManager.getConnection(url, props);
        } catch (Exception e) {
            LOG.error("Phoenix connection error, ", e);
            throw new SQLException(e);
        }
    }

    public static void init() throws IOException {
        // Default load from conf directory
        conf = HBaseConfiguration.create();
        //In Windows environment
        String userdir = PhoenixClientUtil.class.getClassLoader().getResource("conf").getPath() + File.separator;
        //In Linux environment
        //String userdir = System.getProperty("user.dir") + File.separator + "conf" + File.separator;
        conf.addResource(new Path(userdir + "core-site.xml"), false);
        conf.addResource(new Path(userdir + "hdfs-site.xml"), false);
        conf.addResource(new Path(userdir + "hbase-site.xml"), false);
        setProperties(conf);

    }

    private static void setProperties(Configuration conf) {
        Iterator<Map.Entry<String, String>> it = conf.iterator();
        while (it.hasNext()) {
            Map.Entry<String, String> entry = it.next();
            props.put(entry.getKey(), entry.getValue());
        }
    }
}
