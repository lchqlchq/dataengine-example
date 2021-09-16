# H3C DataEngine HDFS example使用步骤

### 未开启kerberos集群使用
1. 登录集群管理界面，下载Hive配置文件： **hive-site.xml** ，并将下载好的配置文件，放置在resources目录。注意需要提前按照hosts文件配置本地集群主机IP映射；
2. 如果集群开启权限，可能会由于权限问题无法操作，需要修改 **USERNAME** 为有权限用户，在代码中修改 **USERNAME** 变量即可，用户名可以从集群的用户管理中获取。、
如果不修改Hive將以匿名用戶執行SQL導致出現無權限問題。

### 开启kerberos集群使用
1. 登录集群管理界面，下载Hive配置文件： **hive-site.xml** ，并将下载好的配置文件，放置在resources目录。注意需要提前按照hosts文件配置本地集群主机IP映射；
2. 集群管理用户管理页面，下载用户认证文件，放置在resources目录；
3. 进入JDBCExample代碼，修改 **PRNCIPAL_NAME** 和 **PATH_TO_KEYTAB** 为具体使用用户名和keytab文件名
4. 执行JDBCExample操作样例程序