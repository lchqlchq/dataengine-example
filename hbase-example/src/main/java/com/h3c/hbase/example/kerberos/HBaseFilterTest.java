package com.h3c.hbase.example.kerberos;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

public class HBaseFilterTest {
    private static final Logger LOG = LoggerFactory.getLogger(HBaseFilterTest.class.getName());

    // 认证信息，具体使用时修改为对应集群的用户名和keytab即可
    private static String principal = "hadoop";
    private static String keytabName = "hadoop.keytab";
    private static String krb5Name = "krb5.conf";

    public static void main(String[] args) {
        try {
            Configuration conf = LoginUtil.init();
            String userName = principal;
            //In Windows environment
            String userdir = HBaseSample.class.getClassLoader().getResource("conf").getPath() + File.separator;
            //In Linux environment
            //String userdir = System.getProperty("user.dir") + File.separator + "conf" + File.separator;

            String userKeytabFile = userdir + keytabName;
            String krb5File = userdir + krb5Name;

            LoginUtil.login(userName, userKeytabFile, krb5File, conf);

            HBaseSample hBaseSample = new HBaseSample(conf);
            hBaseSample.dropTable();
            hBaseSample.testCreateTable();

            hBaseSample.testPut();
            // 行键过滤器
            hBaseSample.testRowFilter();
            // 单列过滤器
            hBaseSample.testSingleColumnValueFilter();
            // 过滤器组合
            hBaseSample.testFilterList();

            hBaseSample.dropTable();

        } catch(IOException e) {

        }
    }
}
