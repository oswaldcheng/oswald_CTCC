package com.oswald.utils;

import java.util.LinkedHashMap;
import java.util.Map;
/**
 * 用于缓存已知的维度id，减少对mysql的操作次数，提高效率
 *
 * @ClassName LRUCache
 * @Description TODO
 * @Author Oswald
 * @Date 2019/2/25
 * @Version V1.0
 **/
public class LRUCache extends LinkedHashMap<String, Integer> {
    private static final long serialVersionUID = 1L;
    protected int maxElements;

    public LRUCache(int maxSize) {
        super(maxSize, 0.75F, true);
        this.maxElements = maxSize;
    }

    /**
     * (non-Javadoc)
     *
     * @see java.util.LinkedHashMap#removeEldestEntry(java.util.Map.Entry)
     */
    @Override
    protected boolean removeEldestEntry(Map.Entry<String, Integer> eldest) {
        return (size() > this.maxElements);
    }
}