package com.peng.framework.frameworkrocketmqstarter.constants;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;

import java.io.Serializable;

public class RocketMqContent implements Serializable {
    private static final long serialVersionUID = 1L;

    public RocketMqContent() {
    }

    public String toString() {
        return JSON.toJSONString(this, new SerializerFeature[]{SerializerFeature.NotWriteDefaultValue});
    }
}
