package com.h3c.phoenix.example.normal;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.*;

public class PhoenixDataGeneratorTool {
    private static final Log LOG = LogFactory.getLog(PhoenixDataGeneratorTool.class);

    private static final String tableName = "MixWirteAndDeleteTest";
    private static final String indexName = "MixWirteAndDeleteTest_IDX";
    private static int columns = 20;
    private static int nThreads = 10;
    private static final int batchSize = 200;
    private static int numRecords = 10000;
    private static int deletedKeyEachRecords = 200;

    private static final Map<Integer, String> columnNameMap = new HashMap<Integer, String>();
    static {
        columnNameMap.put(1, "A");
        columnNameMap.put(2, "B");
        columnNameMap.put(3, "C");
        columnNameMap.put(4, "D");
        columnNameMap.put(5, "E");
        columnNameMap.put(6, "F");
        columnNameMap.put(7, "G");
        columnNameMap.put(8, "H");
        columnNameMap.put(9, "I");
        columnNameMap.put(10, "J");
        columnNameMap.put(11, "K");
        columnNameMap.put(12, "L");
        columnNameMap.put(13, "M");
        columnNameMap.put(14, "N");
        columnNameMap.put(15, "O");
        columnNameMap.put(16, "P");
        columnNameMap.put(17, "Q");
        columnNameMap.put(18, "R");
        columnNameMap.put(19, "S");
        columnNameMap.put(20, "T");
    }
    public static void main(String[] args) throws Exception{
        LOG.info("Starting...");
        //初始化配置文件
        PhoenixClientUtil.init();
        if (args.length == 0 ){
            LOG.info("thread number 10 and record number is 10000!");
        } else if (args.length == 1) {
            nThreads = Integer.valueOf(args[1]);
        } else if (args.length == 2) {
            nThreads = Integer.valueOf(args[1]);
            numRecords = Integer.valueOf(args[2]);
        } else {
            LOG.error("Param error, only support 1 or 2, first is thread number(10 if not set), second is record number(10000 if not set)");
            throw new IllegalArgumentException("Param error, only support 1 or 2, first is thread number(10 if not set), second is record number(10000 if not set)");
        }

        // 创建表和索引
        createTableAndIndex();
        Thread.sleep(2000);
        long startTime = System.currentTimeMillis();
        // 多线程写入数据
        write();
        long endTime = System.currentTimeMillis();

        LOG.info("write data cost time: " + (endTime - startTime));


    }

    public static void createTableAndIndex() throws SQLException {
        Connection conn = PhoenixClientUtil.getConnection();
        conn.createStatement().execute("DROP TABLE IF EXISTS " + tableName);
        conn.createStatement().executeUpdate(
                "CREATE TABLE "  + tableName + " ("
                        + "A VARCHAR NOT NULL PRIMARY KEY,"
                        + "B VARCHAR,"
                        + "C VARCHAR,"
                        + "D VARCHAR,"
                        + "E VARCHAR,"
                        + "F VARCHAR,"
                        + "G VARCHAR,"
                        + "H VARCHAR,"
                        + "I VARCHAR,"
                        + "J VARCHAR,"
                        + "K VARCHAR,"
                        + "L VARCHAR,"
                        + "M VARCHAR,"
                        + "N VARCHAR,"
                        + "O VARCHAR,"
                        + "P VARCHAR,"
                        + "Q VARCHAR,"
                        + "R VARCHAR,"
                        + "S VARCHAR,"
                        + "T VARCHAR"
                        + ") COLUMN_ENCODED_BYTES = 0, SALT_BUCKETS=4");
        conn.createStatement().executeUpdate("CREATE INDEX " + indexName + " on "  + tableName + " (C,D,E) INCLUDE(F, G)");
        LOG.info("Created data table and index table.");
        conn.close();
    }

    public static void write() throws Exception{
        ExecutorService threadPool = Executors.newFixedThreadPool(nThreads);
        List<Future<String>> tasks = new ArrayList<Future<String>>();
        for (int i = 0; i < nThreads ; i++) {
            final String threadName = "Client-" + i;
            tasks.add(threadPool.submit(new Callable<String>() {
                @Override
                public String call() throws Exception {
                    Connection con = PhoenixClientUtil.getConnection();
                    Statement stmt = con.createStatement();

                    String deletedKey = null;
                    String upsertKey = null;

                    for (int i = 0; i < numRecords / nThreads; i++) {
                        StringBuilder upsertBuilder =  new StringBuilder("UPSERT INTO " + tableName + "(");
                        //boolean[] isOmittedCols = isOmittedColumns();
                        for (int j = 1; j <= columns; j++) {
                            //if (isOmittedCols[j - 1]) {
                            upsertBuilder.append(columnNameMap.get(j));
                            upsertBuilder.append(",");
                            //}
                        }
                        upsertBuilder.setLength(upsertBuilder.length() - 1);
                        upsertBuilder.append(") VALUES (");

                        Random random = new Random();
                        for (int j = 1; j <= columns; j++) {
                            String v = "";
                            if(j == 1) {
                                v = UUID.randomUUID().toString();
                                if (i > 0 && i % deletedKeyEachRecords == 0) {
                                    upsertKey = v;
                                }
                            } else {
                                v = String.valueOf(random.nextInt(numRecords));

                            }
                            // 1-2 null value
                            if (j != 1 && j % (random.nextInt(columns * 5) + 1) == 0) {
                                upsertBuilder.append("null");
                            } else {
                                upsertBuilder.append("'");
                                upsertBuilder.append(v);
                                upsertBuilder.append("'");
                                if (i > 0 && i % deletedKeyEachRecords == 0) {
                                    deletedKey = v;
                                }
                            }
                            upsertBuilder.append(',');
                            // }
                        }
                        upsertBuilder.setLength(upsertBuilder.length() - 1);
                        upsertBuilder.append(')');
                        String upsert = upsertBuilder.toString();

                        stmt.executeUpdate(upsert);
                        if (i % batchSize == 0) {
                            con.commit();
                        }

                        // delay to commit at the next iterate.
                        if (i > 0 && i % deletedKeyEachRecords == 0) {
                            String delete = "DELETE FROM " + tableName + " WHERE A = '" + deletedKey + "'";
                            stmt.execute(delete);
                            String b = String.valueOf(random.nextInt(numRecords));
                            String c = String.valueOf(random.nextInt(numRecords));

                            String update = "UPSERT INTO " + tableName + " (A,B,C) VALUE('" +  upsertKey + "', '" + b + "', '" + c + "')";
                            stmt.executeUpdate(upsert);
                            // bw.append(delete + ";\n");
                        }
                    }
                    con.commit();
                    con.close();
                    return threadName ;
                }
            }));
        }
        for (Future f : tasks) {
            System.out.println("task %s is done." + f.get());
        }

        threadPool.shutdown();
        threadPool.awaitTermination(30, TimeUnit.SECONDS);
        LOG.info("wrote finished!");
    }
}
