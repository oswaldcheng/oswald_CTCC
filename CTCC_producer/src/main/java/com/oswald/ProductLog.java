package com.oswald;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @ClassName ProductLog
 * @Description TODO
 * @Author Oswald
 * @Date 2019/2/24
 * @Version V1.0
 **/
public class ProductLog {

    /**
     *起始时间、结束时间
     */
    private String startTime = "2018-01-01";
    private String endTime = "2018-12-31";

    /**
     * 存放用于生产的电话号码 "17078388295"
     */
    private List<String> phoneList = new ArrayList<>();
    /**
     *  存放用于生产的电话号码和姓名 : "17078388295", "李雁"
     */
    private Map<String, String> phoneNameMap = new HashMap<>();

    /**
     * 初始化信息
     *
     * <p>正常该部分信息应该来源于数据库
     */
    public void initPhone(){
        phoneList.add("17078388295");
        phoneList.add("13980337439");
        phoneList.add("14575535933");
        phoneList.add("19902496992");
        phoneList.add("18549641558");
        phoneList.add("17005930322");
        phoneList.add("18468618874");
        phoneList.add("18576581848");
        phoneList.add("15978226424");
        phoneList.add("15542823911");
        phoneList.add("17526304161");
        phoneList.add("15422018558");
        phoneList.add("17269452013");
        phoneList.add("17764278604");
        phoneList.add("15711910344");
        phoneList.add("15714728273");
        phoneList.add("16061028454");
        phoneList.add("16264433631");
        phoneList.add("17601615878");
        phoneList.add("15897468949");

        phoneNameMap.put("17078388295", "李雁");
        phoneNameMap.put("13980337439", "卫艺");
        phoneNameMap.put("14575535933", "仰莉");
        phoneNameMap.put("19902496992", "陶欣悦");
        phoneNameMap.put("18549641558", "施梅梅");
        phoneNameMap.put("17005930322", "金虹霖");
        phoneNameMap.put("18468618874", "魏明艳");
        phoneNameMap.put("18576581848", "华贞");
        phoneNameMap.put("15978226424", "华啟倩");
        phoneNameMap.put("15542823911", "仲采绿");
        phoneNameMap.put("17526304161", "卫丹");
        phoneNameMap.put("15422018558", "戚丽红");
        phoneNameMap.put("17269452013", "何翠柔");
        phoneNameMap.put("17764278604", "钱溶艳");
        phoneNameMap.put("15711910344", "钱琳");
        phoneNameMap.put("15714728273", "缪静欣");
        phoneNameMap.put("16061028454", "焦秋菊");
        phoneNameMap.put("16264433631", "吕访琴");
        phoneNameMap.put("17601615878", "沈丹");
        phoneNameMap.put("15897468949", "褚美丽");
    }

    /**
     * 生产数据(格式):16264433631,15714728273,2018-12-21 15:55:45,1595
     * <p>数据形式对应字段名：caller,callee,buildTime,duration
     * <p>               翻译 :主叫,被叫,通话建立时间,通话持续时间
     * @return
     */
    public String product(){
        // 主叫、被叫电话号
        String caller = null;
        String callee = null;

        // 主叫、被叫姓名
        String callerName = null;
        String calleeName = null;

        // 生成助教的随机索引
        int callerIndex = (int) (Math.random() * phoneList.size());
        // 通过随机索引获得主叫电话号码
        caller = phoneList.get(callerIndex);
        // 通过主叫号码,获得主叫姓名
        callerName = phoneNameMap.get(caller);

        while (true){
            // 生成被叫的随机索引
            int calleeIndex = (int) (Math.random() * phoneList.size());
            // 通过随机索引获得被叫电话号码
            callee = phoneList.get(calleeIndex);
            // 通过被叫号码,获得被叫姓名
            calleeName = phoneNameMap.get(callee);
            // 去重判断、防止自己给自己打电话
            if (!caller.equals(callee)) {
                break;
            }
        }

        //第三个参数:随机产生通话建立时间
        String buildTime = randomBuildTime(startTime, endTime);
        // 随机产生通话持续时间
        DecimalFormat df = new DecimalFormat("0000");

        String duration = df.format((int) (30 * 60 * Math.random()));

        StringBuilder sb = new StringBuilder();
        sb.append(caller + ",").append(callee + ",").append(buildTime + ",").append(duration);
        return sb.toString();
    }

    /**
     * 随机产生时间
     *
     * <p>startTimeTS + (endTimeTs - startTimeTs) * Math.random();
     * @param startTime 开始时间段
     * @param endTime 结束时间段
     * @return yyyy-MM-dd HH:mm:ss
     */
    private String randomBuildTime(String startTime, String endTime) {
        try {
            SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd");
            Date startDate = sdf1.parse(startTime);
            Date endDate = sdf1.parse(endTime);
            //结束时间段大于开始时间段
            if (endDate.getTime() <= startDate.getTime()) {
                return null;
            }

            //随机通话建立时间得Long型 获取介于开始与结束时间段的时间
            long randomTS = startDate.getTime() + (long) ((endDate.getTime() - startDate.getTime()) * Math.random());
            Date resultDate = new Date(randomTS);
            SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String reultTimeString = sdf2.format(resultDate);
            return reultTimeString;
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }


    /**
     * 将日志写入文件
     *
     * @param filePath
     */
    public void writeLog(String filePath){
        try {
            //追加方式
            OutputStreamWriter osw = new OutputStreamWriter(new FileOutputStream(filePath, true));
            while (true){
                Thread.sleep(200);
                String log = product();
                System.out.println(log);
                osw.write(log + "\n");
                // 需要手动flush,确保每条数据都写入文件一次
                osw.flush();
            }

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}
