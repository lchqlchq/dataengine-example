package com.h3c.hbase.example.kerberos;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.util.KerberosUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.AppConfigurationEntry.LoginModuleControlFlag;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

/**
 * 功能描述
 * LoginUtil
 *
 * @since 2013
 */

public class LoginUtil {

    private static final Logger LOG = LoggerFactory.getLogger(LoginUtil.class.getName());


    private static final String JAVA_SECURITY_KRB5_CONF_KEY = "java.security.krb5.conf";


    private static final String LOGIN_FAILED_CAUSE_PASSWORD_WRONG =
        "(wrong password) keytab file and user not match, you can kinit -k -t keytab user in client server to"
            + " check";

    private static final String LOGIN_FAILED_CAUSE_TIME_WRONG =
        "(clock skew) time of local server and remote server not match, please check ntp to remote server";

    private static final String LOGIN_FAILED_CAUSE_AES256_WRONG =
        "(aes256 not support) aes256 not support by default jdk/jre, need copy local_policy.jar and"
            + " US_export_policy.jar from remote server in path /opt/huawei/Bigdata/jdk/jre/lib/security";

    private static final String LOGIN_FAILED_CAUSE_PRINCIPAL_WRONG =
        "(no rule) principal format not support by default, need add property hadoop.security.auth_to_local(in"
            + " core-site.xml) value RULE:[1:$1] RULE:[2:$1]";

    private static final String LOGIN_FAILED_CAUSE_TIME_OUT =
        "(time out) can not connect to kdc server or there is fire wall in the network";

    /**
     * login function
     *
     * @param userPrincipal  user principal.
     * @param userKeytabPath user keytab file path.
     * @param krb5ConfPath krb5 config file path.
     * @param conf configuration object.
     * @throws IOException
     */
    public static synchronized void login(
        String userPrincipal, String userKeytabPath, String krb5ConfPath, Configuration conf) throws IOException {
        // 1.check input parameters
        if ((userPrincipal == null) || (userPrincipal.length() <= 0)) {
            LOG.error("input userPrincipal is invalid.");
            throw new IOException("input userPrincipal is invalid.");
        }

        if ((userKeytabPath == null) || (userKeytabPath.length() <= 0)) {
            LOG.error("input userKeytabPath is invalid.");
            throw new IOException("input userKeytabPath is invalid.");
        }

        if ((krb5ConfPath == null) || (krb5ConfPath.length() <= 0)) {
            LOG.error("input krb5ConfPath is invalid.");
            throw new IOException("input krb5ConfPath is invalid.");
        }

        if ((conf == null)) {
            LOG.error("input conf is invalid.");
            throw new IOException("input conf is invalid.");
        }

        // 2.check file exsits
        File userKeytabFile = new File(userKeytabPath);
        if (!userKeytabFile.exists()) {
            LOG.error("userKeytabFile(" + userKeytabFile.getCanonicalPath() + ") does not exsit.");
            throw new IOException("userKeytabFile(" + userKeytabFile.getCanonicalPath() + ") does not exsit.");
        }
        if (!userKeytabFile.isFile()) {
            LOG.error("userKeytabFile(" + userKeytabFile.getCanonicalPath() + ") is not a file.");
            throw new IOException("userKeytabFile(" + userKeytabFile.getCanonicalPath() + ") is not a file.");
        }

        File krb5ConfFile = new File(krb5ConfPath);
        if (!krb5ConfFile.exists()) {
            LOG.error("krb5ConfFile(" + krb5ConfFile.getCanonicalPath() + ") does not exsit.");
            throw new IOException("krb5ConfFile(" + krb5ConfFile.getCanonicalPath() + ") does not exsit.");
        }
        if (!krb5ConfFile.isFile()) {
            LOG.error("krb5ConfFile(" + krb5ConfFile.getCanonicalPath() + ") is not a file.");
            throw new IOException("krb5ConfFile(" + krb5ConfFile.getCanonicalPath() + ") is not a file.");
        }

        // 3.set and check krb5config
        setKrb5Config(krb5ConfFile.getCanonicalPath());
        setConfiguration(conf);

        // 4.login and check for hadoop
        loginHadoop(userPrincipal, userKeytabFile.getCanonicalPath());
        LOG.info("Login success!!!!!!!!!!!!!!");
    }

    private static void setConfiguration(Configuration conf) throws IOException {
        UserGroupInformation.setConfiguration(conf);
    }

    /**
     * setKrb5Config
     *
     * @param krb5ConfFile krb5 config file value
     * @throws IOException
     */

    public static void setKrb5Config(String krb5ConfFile) throws IOException {
        System.setProperty(JAVA_SECURITY_KRB5_CONF_KEY, krb5ConfFile);
        String ret = System.getProperty(JAVA_SECURITY_KRB5_CONF_KEY);
        if (ret == null) {
            LOG.error(JAVA_SECURITY_KRB5_CONF_KEY + " is null.");
            throw new IOException(JAVA_SECURITY_KRB5_CONF_KEY + " is null.");
        }
        if (!ret.equals(krb5ConfFile)) {
            LOG.error(JAVA_SECURITY_KRB5_CONF_KEY + " is " + ret + " is not " + krb5ConfFile + ".");
            throw new IOException(JAVA_SECURITY_KRB5_CONF_KEY + " is " + ret + " is not " + krb5ConfFile + ".");
        }
    }

    private static void loginHadoop(String principal, String keytabFile) throws IOException {
        try {
            UserGroupInformation.loginUserFromKeytab(principal, keytabFile);
        } catch (IOException e) {
            LOG.error("login failed with " + principal + " and " + keytabFile + ".");
            LOG.error("perhaps cause 1 is " + LOGIN_FAILED_CAUSE_PASSWORD_WRONG + ".");
            LOG.error("perhaps cause 2 is " + LOGIN_FAILED_CAUSE_TIME_WRONG + ".");
            LOG.error("perhaps cause 3 is " + LOGIN_FAILED_CAUSE_AES256_WRONG + ".");
            LOG.error("perhaps cause 4 is " + LOGIN_FAILED_CAUSE_PRINCIPAL_WRONG + ".");
            LOG.error("perhaps cause 5 is " + LOGIN_FAILED_CAUSE_TIME_OUT + ".");

            throw e;
        }
    }
}
