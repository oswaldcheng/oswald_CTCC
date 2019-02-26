package com.oswald.converter;

import com.oswald.kv.base.BaseDimension;

/**
 * 转化接口，用于根据传入的维度对象，得到该维度对象对应的数据库主键id
 *
 * @Interface DimensionConverter
 * @Description TODO
 * @Author Oswald
 * @Date 2019/2/25
 * @Version V1.0
 **/
public interface DimensionConverter {
    /**
     * 基本维度定义
     *
     * @param dimension
     * @return
     */
    int getDimensionID(BaseDimension dimension);
}
