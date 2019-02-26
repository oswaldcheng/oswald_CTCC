package com.oswald;

/**
 * @ClassName ProductDriver
 * @Description 调用日志生产
 * @Author Oswald
 * @Date 2019/2/24
 * @Version V1.0
 **/
public class ProductDriver {
    public static void main(String[] args) {
        if(args == null || args.length<=0){
            System.out.println("没这个路径");
            return;
        }
        ProductLog productLog = new ProductLog();
        productLog.initPhone();
        productLog.writeLog(args[0]);
    }
}
