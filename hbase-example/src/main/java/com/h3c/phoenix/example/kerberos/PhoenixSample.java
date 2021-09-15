
package com.h3c.phoenix.example.kerberos;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.sql.*;
import java.util.Iterator;
import java.util.Map.Entry;

/**
 * 功能描述
 * Phoenix development description sample code,
 * which describes how to use Phoenix APIs to
 * access HBase clusters and perform SQL operations.
 *
 * @since 2013
 */

public class PhoenixSample {
    private static final Logger LOG = LoggerFactory.getLogger(PhoenixSample.class.getName());
    Configuration conf = null;
    java.util.Properties props = new java.util.Properties();
    // 认证信息，具体使用时修改为对应集群的用户名和keytab即可
    private static String principal = "hadoop";
    private static String keytabName = "hadoop.keytab";
    private static String krb5Name = "krb5.conf";

    public PhoenixSample(Configuration conf) {
        this.conf = conf;
        Iterator<Entry<String, String>> it = conf.iterator();
        while (it.hasNext()) {
            Entry<String, String> entry = it.next();
            props.put(entry.getKey(), entry.getValue());
        }
    }

    /**
     * PhoenixSample test
     */
    public void test() {
        testCreateTable();
        testPut();
        testSelect();
        testDrop();
    }

    /**
     * Create Table
     */
    public void testCreateTable() {
        LOG.info("Entering testCreateTable.");
        String url = getUrl();
        // Create table
        String createTableSQL =
            "CREATE TABLE IF NOT EXISTS TEST (id integer not null primary key, name varchar, "
                + "account char(6), birth date)";
        try (Connection conn = DriverManager.getConnection(url, props);
            Statement stat = conn.createStatement()) {
            // Execute Create SQL
            stat.executeUpdate(createTableSQL);
            LOG.info("Create table successfully.");
        } catch (SQLException e) {
            LOG.error("Create table failed.", e);
        }
        LOG.info("Exiting testCreateTable.");
    }

    /**
     * Put data
     */
    public void testPut() {
        LOG.info("Entering testPut.");
        String url = getUrl();
        // Insert
        String upsertSQL =
            "UPSERT INTO TEST VALUES(1,'John','100000', TO_DATE('1980-01-01','yyyy-MM-dd'))";
        try (Connection conn = DriverManager.getConnection(url, props);
            Statement stat = conn.createStatement()) {
            // Execute Update SQL
            stat.executeUpdate(upsertSQL);
            conn.commit();
            LOG.info("Put successfully.");
        } catch (SQLException e) {
            LOG.error("Put failed.", e);
        }
        LOG.info("Exiting testPut.");
    }

    /**
     * Select Data
     */
    public void testSelect() {
        LOG.info("Entering testSelect.");
        String url = getUrl();
        // Query
        String querySQL = "SELECT * FROM TEST WHERE id = ?";
        Connection conn = null;
        PreparedStatement preStat = null;
        Statement stat = null;
        ResultSet result = null;
        try {
            // Create Connection
            conn = DriverManager.getConnection(url, props);
            // Create Statement
            stat = conn.createStatement();
            // Create PrepareStatement
            preStat = conn.prepareStatement(querySQL);
            // Execute query
            preStat.setInt(1, 1);
            result = preStat.executeQuery();
            // Get result
            while (result.next()) {
                int id = result.getInt("id");
                String name = result.getString(1);
                LOG.info("id: {} name: {}", id, name);
            }
            LOG.info("Select successfully.");
        } catch (SQLException e) {
            LOG.error("Select failed.", e);
        } finally {
            if (null != result) {
                try {
                    result.close();
                } catch (SQLException e2) {
                    LOG.error("Result close failed.", e2);
                }
            }
            if (null != stat) {
                try {
                    stat.close();
                } catch (SQLException e2) {
                    LOG.error("Stat close failed.", e2);
                }
            }
            if (null != conn) {
                try {
                    conn.close();
                } catch (SQLException e2) {
                    LOG.error("Connection close failed.", e2);
                }
            }
        }
        LOG.info("Exiting testSelect.");
    }

    private String getUrl() {
        return "jdbc:phoenix:" + conf.get("hbase.zookeeper.quorum") + ":" + conf.get("zookeeper.znode.parent");
    }

    /**
     * Drop Table
     */
    public void testDrop() {
        LOG.info("Entering testDrop.");
        String url = getUrl();
        // Delete table
        String dropTableSQL = "DROP TABLE TEST";

        try (Connection conn = DriverManager.getConnection(url, props);
            Statement stat = conn.createStatement()) {
            stat.executeUpdate(dropTableSQL);
            LOG.info("Drop successfully.");
        } catch (SQLException e) {
            LOG.error("Drop failed.", e);
        }
        LOG.info("Exiting testDrop.");
    }

    public static void main(String[] args) throws  Exception{
        Configuration conf = HBaseConfiguration.create();
        //In Windows environment
        String userdir = PhoenixSample.class.getClassLoader().getResource("conf").getPath() + File.separator;
        //In Linux environment
        //String userdir = System.getProperty("user.dir") + File.separator + "conf" + File.separator;
        conf.addResource(new Path(userdir + "core-site.xml"), false);
        conf.addResource(new Path(userdir + "hdfs-site.xml"), false);
        conf.addResource(new Path(userdir + "hbase-site.xml"), false);
        PhoenixSample phx = new PhoenixSample(conf);

        String userName = principal;
        String userKeytabFile = userdir + keytabName;
        String krb5File = userdir + krb5Name;
        System.setProperty("java.security.krb5.conf", krb5File);
        UserGroupInformation.setConfiguration(conf);
        UserGroupInformation.loginUserFromKeytab(userName, userKeytabFile);
        phx.test();

        /*UserGroupInformation ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(userName, userKeytabFile);
        ugi.doAs(new PrivilegedAction<Boolean>() {
            @Override
            public Boolean run() {
                phx.test();
                return Boolean.TRUE;
            }
        });*/
    }
}

