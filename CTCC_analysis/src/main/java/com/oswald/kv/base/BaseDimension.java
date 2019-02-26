package com.oswald.kv.base;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 维度（key）基类
 *
 * @ClassName BaseDimension
 * @Description TODO
 * @Author Oswald
 * @Date 2019/2/25
 * @Version V1.0
 **/
public abstract class BaseDimension implements WritableComparable<BaseDimension> {
    /**
     * 比较
     *
     * @param o
     * @return
     */
    @Override
    public abstract int compareTo(BaseDimension o);

    /**
     * 写数据
     *
     * @param out
     * @throws IOException
     */
    @Override
    public abstract void write(DataOutput out) throws IOException;

    /**
     * 读数据
     *
     * @param in
     * @throws IOException
     */
    @Override
    public abstract void readFields(DataInput in) throws IOException;
}
