package com.oswald.kafka;

import com.oswald.hbase.HBaseDAO;
import com.oswald.utils.PropertiesUtil;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;

/**
 * @ClassName HBaseConsumer
 * @Description TODO
 * @Author Oswald
 * @Date 2019/2/24
 * @Version V1.0
 **/
public class HBaseConsumer {
    public static void main(String[] args) {
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(PropertiesUtil.properties);
        // 核心函数1,订阅主题topics
        kafkaConsumer.subscribe(Arrays.asList(PropertiesUtil.getProperty("kafka.topics")));

        HBaseDAO hd = new HBaseDAO();

        while (true) {
            //核心函数2：long poll，拉取数据
            ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
            for (ConsumerRecord<String, String> cr : records) {
                String value = cr.value();
                // 18576581848,18468618874,2018-07-02 07:30:49,0181
                System.out.println(value);
                hd.put(value);
            }
        }
    }
}
