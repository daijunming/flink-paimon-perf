package com.paimonperf.metadata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Kerberos 认证工具：在访问 Kerberized HDFS / Paimon 仓库前完成 keytab 登录。
 *
 * <p>设计要点：
 * <ul>
 *   <li><b>可选</b>：未配置 principal/keytab 时跳过认证（兼容非 Kerberos 环境）；</li>
 *   <li><b>幂等</b>：进程内只登录一次，重复调用直接返回（避免重复 loginUserFromKeytab）；</li>
 *   <li><b>显式失败</b>：配置了 Kerberos 但登录失败时抛异常，而非吞掉错误继续（避免后续
 *       访问 HDFS 时报隐晦的 GSS 异常）。</li>
 * </ul>
 *
 * <p>登录后返回 Hadoop {@link Configuration}，可供 Paimon CatalogContext 复用，
 * 确保 catalog 内部的 Hadoop FileSystem 也走同一认证上下文。
 */
public final class KerberosAuthenticator {

    private static final Logger LOG = LoggerFactory.getLogger(KerberosAuthenticator.class);

    /** 进程级登录标志，保证幂等（同一 JVM 只登录一次）。 */
    private static volatile boolean loggedIn = false;

    private KerberosAuthenticator() {
    }

    /**
     * 按需执行 Kerberos 登录并返回已认证的 Hadoop Configuration。
     *
     * @param principal  Kerberos principal（如 flink_user@REALM.COM），为空则跳过认证
     * @param keytab     keytab 文件路径，为空则跳过认证
     * @param krb5Conf   krb5.conf 路径，可为空（为空则用系统默认）
     * @return 已配置认证方式的 Hadoop Configuration；未启用 Kerberos 时返回默认 Configuration
     * @throws IllegalStateException 当启用了 Kerberos 但登录失败
     */
    public static synchronized Configuration authenticate(String principal,
                                                          String keytab,
                                                          String krb5Conf) {
        Configuration conf = new Configuration();

        boolean kerberosEnabled = isNotBlank(principal) && isNotBlank(keytab);
        if (!kerberosEnabled) {
            LOG.info("未配置 Kerberos principal/keytab，跳过认证（非 Kerberos 环境）");
            return conf;
        }

        // 设置 krb5.conf（如提供）。须在 UGI.setConfiguration 之前设置 system property。
        if (isNotBlank(krb5Conf)) {
            System.setProperty("java.security.krb5.conf", krb5Conf);
            LOG.info("使用 krb5.conf: {}", krb5Conf);
        }

        conf.set("hadoop.security.authentication", "Kerberos");
        UserGroupInformation.setConfiguration(conf);

        if (loggedIn && UserGroupInformation.isSecurityEnabled()) {
            LOG.info("Kerberos 已登录（幂等跳过），当前用户: {}", currentUserQuietly());
            return conf;
        }

        try {
            UserGroupInformation.loginUserFromKeytab(principal, keytab);
            loggedIn = true;
            LOG.info("Kerberos 登录成功: principal={}, 当前用户={}", principal, currentUserQuietly());
        } catch (IOException e) {
            throw new IllegalStateException(
                    "Kerberos 登录失败: principal=" + principal + ", keytab=" + keytab, e);
        }
        return conf;
    }

    private static String currentUserQuietly() {
        try {
            return UserGroupInformation.getCurrentUser().getUserName();
        } catch (IOException e) {
            return "<unknown>";
        }
    }

    private static boolean isNotBlank(String s) {
        return s != null && !s.trim().isEmpty();
    }
}
