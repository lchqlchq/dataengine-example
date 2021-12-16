package com.h3c.hbase.example.kerberos;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.*;
import java.util.concurrent.*;

public class HBaseGenerator {
    private static final Logger LOG = LoggerFactory.getLogger(HBaseSample.class.getName());

    private static Configuration conf = null;
    // 认证信息，具体使用时修改为对应集群的用户名和keytab即可
    private static String principal = "hadoop";
    private static String keytabName = "hadoop.keytab";
    private static String krb5Name = "krb5.conf";
    public static TableName tableName = null;

    public static Connection conn = null;

    /**
     * Create user info table
     */
    public static void testCreateTable() {
        LOG.info("Entering testCreateTable.");

        // Specify the table descriptor.
        TableDescriptorBuilder htd = TableDescriptorBuilder.newBuilder(tableName);

        // Set the column family name to info.
        ColumnFamilyDescriptorBuilder hcd = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("info"));

        // Set data encoding methods. HBase provides DIFF,FAST_DIFF,PREFIX
        hcd.setDataBlockEncoding(DataBlockEncoding.FAST_DIFF);

        // Set compression methods, HBase provides two default compression
        // methods:GZ and SNAPPY
        // GZ has the highest compression rate,but low compression and
        // decompression effeciency,fit for cold data
        // SNAPPY has low compression rate, but high compression and
        // decompression effeciency,fit for hot data.
        // it is advised to use SANPPY
        hcd.setCompressionType(Compression.Algorithm.SNAPPY);

        htd.setColumnFamily(hcd.build());

        Admin admin = null;
        try {
            // Instantiate an Admin object.
            admin = conn.getAdmin();
            if (!admin.tableExists(tableName)) {
                LOG.info("Creating table...");
                admin.createTable(htd.build());
                LOG.info(admin.getClusterMetrics().toString());
                LOG.info(admin.listNamespaceDescriptors().toString());
                LOG.info("Table created successfully.");
            } else {
                LOG.warn("table already exists");
            }
        } catch (IOException e) {
            LOG.error("Create table failed.", e);
        } finally {
            if (admin != null) {
                try {
                    // Close the Admin object.
                    admin.close();
                } catch (IOException e) {
                    LOG.error("Failed to close admin ", e);
                }
            }
        }
        LOG.info("Exiting testCreateTable.");
    }


    private static Put putData(byte[] familyName, byte[][] qualifiers, List<String> data) {
        Put put = new Put(Bytes.toBytes(data.get(0)));
        put.addColumn(familyName, qualifiers[0], Bytes.toBytes(data.get(1)));
        put.addColumn(familyName, qualifiers[1], Bytes.toBytes(data.get(2)));
        put.addColumn(familyName, qualifiers[2], Bytes.toBytes(data.get(3)));
        put.addColumn(familyName, qualifiers[3], Bytes.toBytes(data.get(4)));
        return put;
    }
    public static String getRandomString(int length){
        String str="abcdefghijklmnopqrstuvwxyz";
        Random random=new Random();
        StringBuffer sb=new StringBuffer();
        for(int i=0;i<length;i++){
            int number=random.nextInt(26);
            sb.append(str.charAt(number));
        }
        return sb.toString();
    }
    /**
     * Insert data
     */
    public static void testPut() {
        LOG.info("Entering testPut.");
        // Specify the column family name.
        byte[] familyName = Bytes.toBytes("info");
        // Specify the column name.
        byte[][] qualifiers = {
                Bytes.toBytes("name"), Bytes.toBytes("gender"), Bytes.toBytes("age"), Bytes.toBytes("address")
        };

        int threadNum = 10;
        int records = 110;
        int batchSize = 100;
        ExecutorService threadPool = Executors.newFixedThreadPool(threadNum);
        List<Future<String>> tasks = new ArrayList<>();
        Random random = new Random();
        String[] sexs = {"male", "femal"};

        for (int i = 0; i < threadNum; i++) {
            final String threadName = "Client-" + i;
            tasks.add(threadPool.submit(new Callable<String>() {
                @Override
                public String call() throws Exception {
                    Table table = null;
                    List<Put> puts = new ArrayList<Put>();
                    try {
                        // Instantiate an HTable object.
                        table = conn.getTable(tableName);
                        for (int r = 0; r < records; r++) {
                            String uid = UUID.randomUUID().toString();
                            String name = getRandomString(8);
                            String sex = sexs[random.nextInt(2)];
                            String age = String.valueOf(random.nextInt(100));
                            // Instantiate a Put object.
                            Put put = putData(familyName, qualifiers,
                                    Arrays.asList(uid, name, sex, age, getRandomString(6), getRandomString(8)));
                            puts.add(put);
                            if (puts.size() == batchSize) {
                                // Submit a put request.
                                table.put(puts);
                                puts.clear();
                            }
                        }
                    } catch (IOException e) {
                        LOG.error("Put failed ", e);
                    } finally {
                        if (table != null) {
                            try {
                                table.put(puts);
                                // Close the HTable object.
                                table.close();
                            } catch (IOException e) {
                                LOG.error("Close table failed ", e);
                            }
                        }
                    }
                    LOG.info("Put successfully.");
                    return threadName;
                }
            }));
        }
        try {
            for (Future f : tasks) {
                System.out.println("task %s is done." + f.get());
            }
            threadPool.shutdown();
            threadPool.awaitTermination(30, TimeUnit.SECONDS);
            LOG.info("Exiting testPut.");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    /**
     * Delete user table
     */
    public static  void dropTable() {
        LOG.info("Entering dropTable.");

        Admin admin = null;
        try {
            admin = conn.getAdmin();
            if (admin.tableExists(tableName)) {
                // Disable the table before deleting it.
                admin.disableTable(tableName);

                // Delete table.
                admin.deleteTable(tableName);
            }
            LOG.info("Drop table successfully.");
        } catch (IOException e) {
            LOG.error("Drop table failed ", e);
        } finally {
            if (admin != null) {
                try {
                    // Close the Admin object.
                    admin.close();
                } catch (IOException e) {
                    LOG.error("Close admin failed ", e);
                }
            }
        }
        LOG.info("Exiting dropTable.");
    }

    private static void login() throws IOException {
        if (User.isHBaseSecurityEnabled(conf)) {
            String userName = principal;
            //In Windows environment
            String userdir = HBaseSample.class.getClassLoader().getResource("conf").getPath() + File.separator;
            //In Linux environment
            //String userdir = System.getProperty("user.dir") + File.separator + "conf" + File.separator;

            String userKeytabFile = userdir + keytabName;
            String krb5File = userdir + krb5Name;

            LoginUtil.login(userName, userKeytabFile, krb5File, conf);
        }
    }

    public static void main(String[] args) {
        try {
            conf = LoginUtil.init();
            login();
        } catch (IOException e) {
            LOG.error("Failed to login because ", e);
            return;
        }

        // test hbase
        try {
            tableName = TableName.valueOf("HBaseGenerator");
            conn = ConnectionFactory.createConnection(conf);
            testCreateTable();
            testPut();
        } catch (IOException e) {
            LOG.error("Failed to test HBase because ", e);
        }
        LOG.info("-----------finish HBase -------------------");
    }


}
