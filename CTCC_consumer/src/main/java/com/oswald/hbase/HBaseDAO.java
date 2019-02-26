package com.oswald.hbase;

import com.oswald.utils.ConnectionInstance;
import com.oswald.utils.HBaseUtil;
import com.oswald.utils.PropertiesUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

/**
 * @ClassName HBaseDAO
 * @Description TODO
 * @Author Oswald
 * @Date 2019/2/24
 * @Version V1.0
 **/
public class HBaseDAO {
    private int regions;
    // 名称空间
    private String namespace;
    // 表名
    private String tableName;
    // 配置文件
    private static final Configuration conf;
    //
    private HTable table;

    private Connection connection;

    //时间转换
    private SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private SimpleDateFormat sdf2 = new SimpleDateFormat("yyyyMMddHHmmss");

    // 缓存集合
    private List<Put> cacheList = new ArrayList<>();
    // 获取hbase配置信息
    static {
        conf = HBaseConfiguration.create();
    }

    // 构造函数
    public HBaseDAO() {
        try {
            regions = Integer.valueOf(PropertiesUtil.getProperty("hbase.calllog.regions"));
            System.out.println("regions:"+regions);
            // 获取名称空间 在这儿
            namespace = PropertiesUtil.getProperty("hbase.calllog.namespace");
            // 获取表名
            tableName = PropertiesUtil.getProperty("hbase.calllog.tablename");
            //
            if (!HBaseUtil.isExistTable(conf, tableName)) {
                HBaseUtil.initNameSpace(conf, namespace);
                HBaseUtil.createTable(conf, tableName, regions, "f1", "f2");
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * value格式 : 18576581848,18468618874,2018-07-02 07:30:49,0181 Rowkey格式 :
     * regionCode_caller_buildTime_callee_flag_duration
     *
     * @param value
     */
    public void put(String value) {
        try {

            // 优化代码，做批处理
            if (cacheList.size() == 0) {
                //获得链接
                connection = ConnectionInstance.getConnection(conf);

                table = (HTable) connection.getTable(TableName.valueOf(tableName));
                /*
                通过调用HTable.setAutoFlush(false)方法可以将HTable写客户端的自动flush关闭，
                这样可以批量写入数据到HBase，而不是有一条put就执行一次更新，只有当put填满客户端写缓存时，
                才实际向HBase服务端发起写请求。默认情况下auto flush是开启的。
                 */
                table.setAutoFlushTo(false);
                //2m
                /*
                通过调用HTable.setWriteBufferSize(writeBufferSize)方法可以设置HTable客户端的写buffer大小，
                如果新设置的buffer小于当前写buffer中的数据时，buffer将会被flush到服务端。其中，
                writeBufferSize的单位是byte字节数，可以根据实际写入数据量的多少来设置该值。
                 */
                table.setWriteBufferSize(2 * 1024 * 1024);
            }
            //切分
            String[] splitOri = value.split(",");
            //主叫
            String caller = splitOri[0];
            //被叫
            String callee = splitOri[1];
            //通话建立时间
            String buildTime = splitOri[2];
            //持续时间
            String duration = splitOri[3];
            //预分区键 散列分区号
            String regionCode = HBaseUtil.genRegionCode(caller, buildTime, regions);

            String buildTimeReplace = sdf2.format(sdf1.parse(buildTime));
            //时间戳
            String buildTimeTS = String.valueOf(sdf1.parse(buildTime).getTime());

            // 生成rowkey 主叫
            String rowkey =
                    HBaseUtil.genRowkey(regionCode, caller, buildTimeReplace, callee, "1", duration);
            // 向表中插入数据
            Put put = new Put(Bytes.toBytes(rowkey));
            put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("caller"), Bytes.toBytes(caller));
            put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("callee"), Bytes.toBytes(callee));
            put.addColumn(
                    Bytes.toBytes("f1"), Bytes.toBytes("buildTimeReplace"), Bytes.toBytes(buildTimeReplace));
            put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("buildTimeTS"), Bytes.toBytes(buildTimeTS));
            put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("flag"), Bytes.toBytes("1"));
            put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("duration"), Bytes.toBytes(duration));

            cacheList.add(put);

            // 生成rowkey 被叫
//            String rowkey2 =
//                    HBaseUtil.genRowkey(regionCode, callee, buildTimeReplace, caller, "0", duration);
//            // 向表中插入数据
//            Put put2 = new Put(Bytes.toBytes(rowkey2));
//            put2.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("caller"), Bytes.toBytes(callee));
//            put2.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("callee"), Bytes.toBytes(caller));
//            put2.addColumn(
//                    Bytes.toBytes("f2"), Bytes.toBytes("buildTimeReplace"), Bytes.toBytes(buildTimeReplace));
//            put2.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("buildTimeTS"), Bytes.toBytes(buildTimeTS));
//            put2.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("flag"), Bytes.toBytes("0"));
//            put2.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("duration"), Bytes.toBytes(duration));
//
//            cacheList.add(put2);
            if (cacheList.size() >= 30) {
                table.put(cacheList);
                table.flushCommits();
                table.close();
                cacheList.clear();
            }

        } catch (ParseException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
