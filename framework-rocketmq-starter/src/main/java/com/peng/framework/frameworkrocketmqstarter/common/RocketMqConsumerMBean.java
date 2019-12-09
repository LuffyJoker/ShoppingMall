package com.peng.framework.frameworkrocketmqstarter.common;

import java.util.List;

public class RocketMqConsumerMBean {
    private List<AbstractRocketMqConsumer> consumers;

    public RocketMqConsumerMBean() {
    }

    public List<AbstractRocketMqConsumer> getConsumers() {
        return this.consumers;
    }

    public void setConsumers(List<AbstractRocketMqConsumer> consumers) {
        this.consumers = consumers;
    }
}

