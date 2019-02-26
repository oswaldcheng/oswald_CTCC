package com.oswald.kv.value;

import com.oswald.kv.base.BaseValue;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
/**
 * 通话次数与通话时长的封装
 *
 * @ClassName CountDurationValue
 * @Description TODO
 * @Author Oswald
 * @Date 2019/2/25
 * @Version V1.0
 **/
@AllArgsConstructor
@NoArgsConstructor
@Setter
@Getter
public class CountDurationValue extends BaseValue {
    // 通话总次数
    private String callSum;
    // 通话 总时间
    private String callDurationSum;

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(callSum);
        out.writeUTF(callDurationSum);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.callSum = in.readUTF();
        this.callDurationSum = in.readUTF();
    }
}
