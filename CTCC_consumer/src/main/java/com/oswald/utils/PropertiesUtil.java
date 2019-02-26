package com.oswald.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * 该类主要用于将常用的项目所需的参数外部化，解耦，方便配置。
 *
 * @ClassName PropertiesUtil
 * @Description TODO
 * @Author Oswald
 * @Date 2019/2/24
 * @Version V1.0
 **/
public class PropertiesUtil {
    public static Properties properties = null;

    /**
     * 加载配置属性
     */
    static {
        InputStream inputStream = ClassLoader.getSystemResourceAsStream("hbase_consumer.properties");
        properties = new Properties();
        try {
            properties.load(inputStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取key对应value
     * @param key
     * @return
     */
    public static String getProperty(String key) {
        return properties.getProperty(key);
    }
}
