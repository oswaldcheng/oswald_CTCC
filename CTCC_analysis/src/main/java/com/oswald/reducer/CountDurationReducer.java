package com.oswald.reducer;

import com.oswald.kv.key.ComDimension;
import com.oswald.kv.value.CountDurationValue;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 数据分析的Reducer类，继承自Reduccer
 *
 * @ClassName CountDurationReducer
 * @Description TODO
 * @Author Oswald
 * @Date 2019/2/25
 * @Version V1.0
 **/
public class CountDurationReducer
        extends Reducer<ComDimension, Text, ComDimension, CountDurationValue> {
    CountDurationValue countDurationValue = new CountDurationValue();

    @Override
    protected void reduce(ComDimension key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        int callSum = 0;
        int callDurationSum = 0;
        for (Text t : values) {
            callSum++;
            callDurationSum += Integer.valueOf(t.toString());
        }
        //转成字符串
        countDurationValue.setCallSum(String.valueOf(callSum));
        countDurationValue.setCallDurationSum(String.valueOf(callDurationSum));

        context.write(key, countDurationValue);
    }
}