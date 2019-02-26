package com.oswald.hbase;

import com.oswald.utils.HBaseUtil;
import com.oswald.utils.PropertiesUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * 用于实现主叫日志插入成功之后，同时插入一条被叫日志
 *
 * @ClassName CalleeWriteObserver
 * @Description TODO
 * @Author Oswald
 * @Date 2019/2/24
 * @Version V1.0
 **/
public class CalleeWriteObserver extends BaseRegionObserver {
    SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");

    @Override
    public void postPut(
            ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability)
            throws IOException {
        // 1、获取你想要操作的目标表的名称
        String targetTableName = PropertiesUtil.getProperty("hbase.calllog.tablename");
        // 2、 获取当前put数据的表   当前操作的表(当前操作的表不一定是想要操作的表)
        String currentTableName = e.getEnvironment().getRegionInfo().getTable().getNameAsString();
        // 3.判断需要操作的表是否就是当前表，如果不是，则return
        if (!targetTableName.equals(currentTableName)) {
            return;
        }

        // regionCode_caller_buildTime_callee_flag_duration
        String oriRowKey = Bytes.toString(put.getRow());
        //切割
        String[] splitOriRowKey = oriRowKey.split("_");
        // 获取flag
        String oldFlag = splitOriRowKey[4];
        // 如果当前插入的是被叫数据，则直接返回  0 “被叫”   1 “主叫”
        if (oldFlag.equals("0")) {
            return;
        }

        // 组装新的数据
        int regions = Integer.valueOf(PropertiesUtil.getProperty("hbase.calllog.regions"));

        String caller = splitOriRowKey[1];
        String callee = splitOriRowKey[3];
        String buildTime = splitOriRowKey[2];
        String duration = splitOriRowKey[5];
        String flag = "0";
        String regionCode = HBaseUtil.genRegionCode(callee, buildTime, regions);

        String calleeRowKey =
                HBaseUtil.genRowkey(regionCode, callee, buildTime, caller, flag, duration);

        // 生成时间戳
        String buildTimeTs = "";
        try {
            buildTimeTs = String.valueOf(sdf.parse(buildTime).getTime());
        } catch (ParseException e1) {
            e1.printStackTrace();
        }

        // 开始存放被叫数据
        Put calleePut = new Put(Bytes.toBytes(calleeRowKey));
        calleePut.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("caller"), Bytes.toBytes(caller));
        calleePut.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("callee"), Bytes.toBytes(callee));
        calleePut.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("build_Time"), Bytes.toBytes(buildTime));
        calleePut.addColumn(
                Bytes.toBytes("f2"), Bytes.toBytes("buildTimeTS"), Bytes.toBytes(buildTimeTs));
        calleePut.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("flag"), Bytes.toBytes(flag));
        calleePut.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("duration"), Bytes.toBytes(duration));

        Table table = e.getEnvironment().getTable(TableName.valueOf(targetTableName));
        table.put(calleePut);
        table.close();
    }
}
