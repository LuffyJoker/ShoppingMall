package com.peng.framework.frameworkrocketmqstarter.common;

import org.apache.rocketmq.client.producer.*;
import org.apache.rocketmq.common.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;

public class DefaultRocketMqProducer {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    private DefaultMQProducer producer;

    public DefaultRocketMqProducer() {
    }

    public boolean sendMessage(Message msg) {
        SendResult sendResult = null;

        try {
            sendResult = this.producer.send(msg);
        } catch (Exception var4) {
            this.logger.error("send message error", var4);
        }

        return this.result(sendResult);
    }

    public void sendMessage(Message msg, SendCallback sendCallback) {
        try {
            this.producer.send(msg, sendCallback);
        } catch (Exception var4) {
            this.logger.error("send message error", var4);
        }

    }

    public boolean sendMessage(List<Message> msg, long timeout) {
        SendResult sendResult = null;

        try {
            sendResult = this.producer.send(msg, timeout);
        } catch (Exception var6) {
            this.logger.error("send message error", var6);
        }

        return this.result(sendResult);
    }

    public boolean sendMessage(List<Message> msg) {
        SendResult sendResult = null;

        try {
            sendResult = this.producer.send(msg);
        } catch (Exception var4) {
            this.logger.error("send message error", var4);
        }

        return this.result(sendResult);
    }

    public boolean sendMessage(Message msg, long timeout) {
        SendResult sendResult = null;

        try {
            sendResult = this.producer.send(msg, timeout);
        } catch (Exception var6) {
            this.logger.error("send message error", var6);
        }

        return this.result(sendResult);
    }

    public boolean sendMessage(Message msg, MessageQueueSelector selector, Object args) {
        SendResult sendResult = null;

        try {
            sendResult = this.producer.send(msg, selector, args);
        } catch (Exception var6) {
            this.logger.error("send message error ", var6);
        }

        return this.result(sendResult);
    }

    private boolean result(SendResult sendResult) {
        boolean result = sendResult != null && sendResult.getSendStatus() == SendStatus.SEND_OK;
        if (!result) {
            this.logger.info("消息投递失败");
        } else {
            this.logger.info("消息：{} 投递成功", sendResult.getMsgId());
        }

        return result;
    }

    public DefaultMQProducer getProducer() {
        return this.producer;
    }

    public void setProducer(DefaultMQProducer producer) {
        this.producer = producer;
    }

    public void destroy() {
        if (Objects.nonNull(this.producer)) {
            this.producer.shutdown();
        }

    }
}

