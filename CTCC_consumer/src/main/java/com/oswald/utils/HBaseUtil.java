package com.oswald.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Iterator;
import java.util.TreeSet;



/**
 * @ClassName HBaseUtil
 * @Description TODO
 * @Author Oswald
 * @Date 2019/2/24
 * @Version V1.0
 **/
public class HBaseUtil {
    /**
     * 判断表是否存在
     *
     * @param conf
     * @param tableName
     * @return
     * @throws IOException
     */
    public static boolean isExistTable(Configuration conf, String tableName) throws IOException {
        // 根据配置创建链接
        Connection connection = ConnectionFactory.createConnection(conf);
        // 操作HBase表  必须创建HBaseAdmin对象
        HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();
        // 判断表是否存在
        boolean result = admin.tableExists(TableName.valueOf(tableName));
        close(admin, connection);
        return result;
    }

    /**
     * 关闭流
     *
     * @param admin
     * @param connection
     * @throws IOException
     */
    private static void close(Admin admin, Connection connection) throws IOException {
        if (admin != null) {
            admin.close();
        }
        if (connection != null) {
            connection.close();
        }
    }

    /**
     * 初始化命名空间
     *
     * @param conf
     * @param namespace
     * @throws IOException
     */
    public static void initNameSpace(Configuration conf, String namespace) throws IOException {
        // 命名空间类似于关系型数据库中的schema，可以想象成文件夹
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();
        // 命名空间描述器
        NamespaceDescriptor build =
                NamespaceDescriptor.create(namespace)
                        .addConfiguration("CREATE_TIME", String.valueOf(System.currentTimeMillis()))
                        .addConfiguration("AUTHOR", "oswald")
                        .build();
        admin.createNamespace(build);
        close(admin, connection);
    }

    /**
     * 创建表:协处理器
     *
     * @param conf 配置
     * @param tableName 表名
     * @param regions region 个数
     * @param columnFamily 列簇
     * @throws IOException
     */
    public static void createTable(
            Configuration conf, String tableName, int regions, String... columnFamily)
            throws IOException {
        //创建链接
        Connection connection = ConnectionFactory.createConnection(conf);
        // 操作HBase表  必须创建HBaseAdmin对象
        Admin admin = connection.getAdmin();

        // 判断表是否存在
        if (isExistTable(conf, tableName)) {
            return;
        }
        // 创建表要先创建表描述器
        HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tableName));

        //创建列族
        for (String cf : columnFamily) {
            // 创建列、HColumnDescriptor列描述器
            htd.addFamily(new HColumnDescriptor(cf));
        }
        htd.addCoprocessor("com.oswald.hbase.CalleeWriteObserver");
        admin.createTable(htd, genSplitKeys(regions));
        System.out.println("创建表成功================================");
        close(admin, connection);
    }

    /**
     * 预分区键
     * 例如：{"00|", "01|", "02|", "03|", "04|", "05|"}
     *
     *
     * @param regions
     * @return
     */
    private static byte[][] genSplitKeys(int regions) {
        // 定义一个存放分区键的数组
        String[] keys = new String[regions];
        //这里默认不会超过两位数的分区，如果超过，需要变更设计，如果需要灵活操作，也需要变更设计
        // regions个数不会超过2位数，一个region能放10G 200万条约等于50 ~ 100MB
        DecimalFormat df = new DecimalFormat("00");
        for (int i = 0; i < regions; i++) {
            //例如：如果regions = 6，则：{"00|", "01|", "02|", "03|", "04|", "05|"}
            keys[i] = df.format(i) + "|";
        }
        byte[][] splitkeys = new byte[regions][];
        // 生成byte[][]类型的分区键的时候,一定是保证分区键是有序的
        TreeSet<byte[]> treeSet = new TreeSet<>(Bytes.BYTES_COMPARATOR);
        for (int i = 0; i < regions; i++) {
            treeSet.add(Bytes.toBytes(keys[i]));
        }

        Iterator<byte[]> splitKeysIterator = treeSet.iterator();
        int index = 0;
        while (splitKeysIterator.hasNext()) {
            byte[] b = splitKeysIterator.next();
            splitkeys[index++] = b;
        }
        return splitkeys;
    }

    /**
     * 数据格式
     *
     * @param caller
     * @param buildTime
     * @param regions 自己后边写
     * @return
     */
    public static String genRegionCode(String caller, String buildTime, int regions) {
        int len = caller.length();
        // 取出电话号码后四位 1846861 8874
        String lastPhone = caller.substring(len - 4);

        // 取年月
        String ym =
                buildTime
                        // 2018-01-23  => 20180123
                        .replaceAll("-", "")
                        .replaceAll(":", "")
                        .replaceAll(" ", "")
                        .substring(0, 6);
        // 做离散操作1 异或  离散，让数据分散，防止数据倾斜
        Integer x = Integer.valueOf(lastPhone) ^ Integer.valueOf(ym);
        // 做离散操作2
        int y = x.hashCode();
        // 生成分区号
        int regionCode = y % regions;
        // 格式化分区号
        DecimalFormat df = new DecimalFormat("00");
        return df.format(regionCode);
    }

    /**
     * 组装RowKey regionCode_caller_buildTime_callee_flag_duration
     *
     * @param regionCode 随机的分区号
     * @param caller 主叫号码
     * @param buildTime 随机建立通话时间
     * @param callee 被叫号码
     * @param flag 标记: 0 / 1
     * @param duration 通话持续时间
     * @return 返回RK
     */
    public static String genRowkey(
            String regionCode,
            String caller,
            String buildTime,
            String callee,
            String flag,
            String duration) {
        //组装RowKey
        StringBuilder sb = new StringBuilder();
        sb.append(regionCode + "_")
                .append(caller + "_")
                .append(buildTime + "_")
                .append(callee + "_")
                .append(flag + "_")
                .append(duration);
        return sb.toString();
    }

}
