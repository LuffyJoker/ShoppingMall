package com.peng.framework.frameworkrocketmqstarter.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(
        prefix = "spring.rocketmq"
)
public class RocketMqProperties {
    static final String PREFIX = "spring.rocketmq";
    private String namesrvAddr;
    private String producerGroupName;
    private int sendMsgTimeout = 3000;
    private int compressMsgBodyOverHowMuch = 4096;
    private int retryTimesWhenSendFailed = 2;
    private int retryTimesWhenSendAsyncFailed = 2;
    private boolean retryAnotherBrokerWhenNotStoreOk = false;
    private int maxMessageSize = 4194304;

    public RocketMqProperties() {
    }

    public String getNamesrvAddr() {
        return this.namesrvAddr;
    }

    public void setNamesrvAddr(String namesrvAddr) {
        this.namesrvAddr = namesrvAddr;
    }

    public String getProducerGroupName() {
        return this.producerGroupName;
    }

    public void setProducerGroupName(String producerGroupName) {
        this.producerGroupName = producerGroupName;
    }

    public int getSendMsgTimeout() {
        return this.sendMsgTimeout;
    }

    public void setSendMsgTimeout(int sendMsgTimeout) {
        this.sendMsgTimeout = sendMsgTimeout;
    }

    public int getCompressMsgBodyOverHowMuch() {
        return this.compressMsgBodyOverHowMuch;
    }

    public void setCompressMsgBodyOverHowMuch(int compressMsgBodyOverHowMuch) {
        this.compressMsgBodyOverHowMuch = compressMsgBodyOverHowMuch;
    }

    public int getRetryTimesWhenSendFailed() {
        return this.retryTimesWhenSendFailed;
    }

    public void setRetryTimesWhenSendFailed(int retryTimesWhenSendFailed) {
        this.retryTimesWhenSendFailed = retryTimesWhenSendFailed;
    }

    public int getRetryTimesWhenSendAsyncFailed() {
        return this.retryTimesWhenSendAsyncFailed;
    }

    public void setRetryTimesWhenSendAsyncFailed(int retryTimesWhenSendAsyncFailed) {
        this.retryTimesWhenSendAsyncFailed = retryTimesWhenSendAsyncFailed;
    }

    public boolean isRetryAnotherBrokerWhenNotStoreOk() {
        return this.retryAnotherBrokerWhenNotStoreOk;
    }

    public void setRetryAnotherBrokerWhenNotStoreOk(boolean retryAnotherBrokerWhenNotStoreOk) {
        this.retryAnotherBrokerWhenNotStoreOk = retryAnotherBrokerWhenNotStoreOk;
    }

    public int getMaxMessageSize() {
        return this.maxMessageSize;
    }

    public void setMaxMessageSize(int maxMessageSize) {
        this.maxMessageSize = maxMessageSize;
    }
}

