package com.peng.framework.frameworkrocketmqstarter.common;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.parser.Feature;
import com.peng.framework.frameworkrocketmqstarter.anno.RocketListenerHandler;
import com.peng.framework.frameworkrocketmqstarter.config.RocketMqProperties;
import com.peng.framework.frameworkrocketmqstarter.constants.ConsumeMode;
import com.peng.framework.frameworkrocketmqstarter.constants.RocketMqTopic;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.MessageSelector;
import org.apache.rocketmq.client.consumer.listener.*;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.*;

@EnableConfigurationProperties({RocketMqProperties.class})
public abstract class AbstractRocketMqConsumer<Topic extends RocketMqTopic, Content> {
    protected final Logger logger = LoggerFactory.getLogger(this.getClass());
    protected Class<Topic> topicClazz;
    protected Type contentClazz;
    private boolean isStarted;
    private Integer consumeThreadMin;
    private Integer consumeThreadMax;
    private ConsumeFromWhere consumeFromWhere;
    private int delayLevelWhenNextConsume = 0;
    private long suspendCurrentQueueTimeMillis = -1L;
    private MessageModel messageModel;
    private ConsumeMode consumeMode;
    private DefaultMQPushConsumer consumer;
    private String consumerGroup;
    private Map<String, Set<String>> annotationsMap;
    private String topic;
    private Set<String> tags;
    private String messageSelector;
    private int consumeMsgMaxSize;
    private int reConsumerTimes = 16;

    public AbstractRocketMqConsumer() {
    }

    public Map<String, Set<String>> subscribeTopicTags(Map<String, Set<String>> customerMap) {
        return null != customerMap ? customerMap : this.annotationsMap;
    }

    public String getConsumerGroup() {
        return this.consumerGroup;
    }

    public abstract boolean consumeMsg(Content var1, MessageExt var2);

    @PostConstruct
    public void init() throws MQClientException {
        Class<? extends AbstractRocketMqConsumer> parentClazz = this.getClass();
        Type genType = parentClazz.getGenericSuperclass();
        Type[] types = ((ParameterizedType) genType).getActualTypeArguments();
        this.topicClazz = (Class) types[0];
        this.contentClazz = types[1];
        RocketListenerHandler handler = (RocketListenerHandler) parentClazz.getAnnotation(RocketListenerHandler.class);
        if (null == handler) {
            throw new IllegalArgumentException("消费者缺少RocketListenerHandler注解！！！");
        } else {
            this.reConsumerTimes = handler.reTryConsume();
            this.consumeMsgMaxSize = handler.consumeMsgMaxSize();
            this.messageModel = handler.type();
            this.consumeFromWhere = handler.consumerFromWhere();
            this.consumeThreadMin = handler.consumeThreadMin();
            this.consumeThreadMax = handler.consumeThreadMax();
            this.consumeMode = handler.consumeMode();
            this.consumerGroup = handler.consumerGroup();
            this.messageSelector = handler.messageSelector();
            this.topic = handler.topic();
            String[] tagsArray = handler.tags();
            this.tags = new HashSet(Arrays.asList(tagsArray));
            if (!handler.reWriteSubscribe()) {
                this.annotationsMap = new HashMap();
                this.annotationsMap.put(this.topic, this.tags);
            }

            if (this.isStarted()) {
                throw new IllegalStateException("Container already Started. " + this.toString());
            } else {
                this.initRocketMQPushConsumer();
            }
        }
    }

    @PreDestroy
    public void destroy() {
        if (Objects.nonNull(this.consumer)) {
            this.consumer.shutdown();
        }

        this.logger.info("consumer shutdown, {}", this.toString());
    }

    private void initRocketMQPushConsumer() throws MQClientException {
        Assert.notNull(this.getConsumerGroup(), "Property 'consumerGroup' is required");
        Assert.notEmpty(this.subscribeTopicTags((Map) null), "SubscribeTopicTags method can't be empty");
        this.consumer = new DefaultMQPushConsumer(this.getConsumerGroup());
        this.consumer.setConsumeMessageBatchMaxSize(this.consumeMsgMaxSize);
        this.consumer.setMaxReconsumeTimes(this.reConsumerTimes <= 0 ? -1 : this.reConsumerTimes);
        if (StringUtils.hasText(this.messageSelector)) {
            this.consumer.subscribe(this.topic, MessageSelector.bySql(this.messageSelector));
        }

        if (this.consumeThreadMax != null) {
            this.consumer.setConsumeThreadMax(this.consumeThreadMax);
        }

        if (this.consumeThreadMax != null && this.consumeThreadMax < this.consumer.getConsumeThreadMin()) {
            this.consumer.setConsumeThreadMin(this.consumeThreadMax);
        }

        this.consumer.setConsumeFromWhere(this.consumeFromWhere);
        this.consumer.setMessageModel(this.messageModel);
        switch (this.consumeMode) {
            case Orderly:
                this.consumer.registerMessageListener(new AbstractRocketMqConsumer.DefaultMessageListenerOrderly());
                break;
            case CONCURRENTLY:
                this.consumer.registerMessageListener(new AbstractRocketMqConsumer.DefaultMessageListenerConcurrently());
                break;
            default:
                throw new IllegalArgumentException("Property 'consumeMode' was wrong.");
        }

    }

    private <T> T parseMsg(byte[] body, Type clazz) {
        T t = null;
        if (body != null) {
            try {
                t = JSON.parseObject(body, clazz, new Feature[0]);
            } catch (Exception var5) {
                this.logger.error("Can not parse to Object", var5);
            }
        }

        return t;
    }

    public Integer getConsumeThreadMin() {
        return this.consumeThreadMin;
    }

    public void setConsumeThreadMin(Integer consumeThreadMin) {
        this.consumeThreadMin = consumeThreadMin;
    }

    public Integer getConsumeThreadMax() {
        return this.consumeThreadMax;
    }

    public void setConsumeThreadMax(Integer consumeThreadMax) {
        this.consumeThreadMax = consumeThreadMax;
    }

    public boolean isStarted() {
        return this.isStarted;
    }

    public void setStarted(boolean started) {
        this.isStarted = started;
    }

    public ConsumeFromWhere getConsumeFromWhere() {
        return this.consumeFromWhere;
    }

    public void setConsumeFromWhere(ConsumeFromWhere consumeFromWhere) {
        this.consumeFromWhere = consumeFromWhere;
    }

    public int getDelayLevelWhenNextConsume() {
        return this.delayLevelWhenNextConsume;
    }

    public void setDelayLevelWhenNextConsume(int delayLevelWhenNextConsume) {
        this.delayLevelWhenNextConsume = delayLevelWhenNextConsume;
    }

    public long getSuspendCurrentQueueTimeMillis() {
        return this.suspendCurrentQueueTimeMillis;
    }

    public void setSuspendCurrentQueueTimeMillis(long suspendCurrentQueueTimeMillis) {
        this.suspendCurrentQueueTimeMillis = suspendCurrentQueueTimeMillis;
    }

    public MessageModel getMessageModel() {
        return this.messageModel;
    }

    public void setMessageModel(MessageModel messageModel) {
        this.messageModel = messageModel;
    }

    public ConsumeMode getConsumeMode() {
        return this.consumeMode;
    }

    public void setConsumeMode(ConsumeMode consumeMode) {
        this.consumeMode = consumeMode;
    }

    public DefaultMQPushConsumer getConsumer() {
        return this.consumer;
    }

    public class DefaultMessageListenerOrderly implements MessageListenerOrderly {
        public DefaultMessageListenerOrderly() {
        }

        public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
            Iterator var3 = msgs.iterator();
            if (var3.hasNext()) {
                MessageExt messageExt = (MessageExt) var3.next();

                try {
                    if (AbstractRocketMqConsumer.this.consumeMsg(AbstractRocketMqConsumer.this.parseMsg(messageExt.getBody(), AbstractRocketMqConsumer.this.contentClazz), messageExt)) {
                        AbstractRocketMqConsumer.this.logger.debug("Consume message: {}", messageExt.getMsgId());
                        return ConsumeOrderlyStatus.SUCCESS;
                    } else {
                        AbstractRocketMqConsumer.this.logger.info("Reject message:{}", messageExt.getMsgId());
                        return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
                    }
                } catch (Exception var6) {
                    AbstractRocketMqConsumer.this.logger.warn("Consume message failed. messageExt:{}", messageExt, var6);
                    context.setSuspendCurrentQueueTimeMillis(AbstractRocketMqConsumer.this.suspendCurrentQueueTimeMillis);
                    return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
                }
            } else {
                return ConsumeOrderlyStatus.SUCCESS;
            }
        }
    }

    public class DefaultMessageListenerConcurrently implements MessageListenerConcurrently {
        public DefaultMessageListenerConcurrently() {
        }

        public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
            Iterator var3 = msgs.iterator();
            if (var3.hasNext()) {
                MessageExt messageExt = (MessageExt) var3.next();

                try {
                    if (messageExt.getReconsumeTimes() > AbstractRocketMqConsumer.this.reConsumerTimes) {
                        AbstractRocketMqConsumer.this.logger.info("Message achieve reTryTimes, delivery to DLQ queue {},{},{},{}", new Object[]{AbstractRocketMqConsumer.this.reConsumerTimes, messageExt.getMsgId(), messageExt.getTopic(), messageExt.getTags()});
                    }

                    if (AbstractRocketMqConsumer.this.consumeMsg(AbstractRocketMqConsumer.this.parseMsg(messageExt.getBody(), AbstractRocketMqConsumer.this.contentClazz), messageExt)) {
                        AbstractRocketMqConsumer.this.logger.info("Consume message: {},{},{}", new Object[]{messageExt.getMsgId(), messageExt.getTopic(), messageExt.getTags()});
                        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                    } else {
                        AbstractRocketMqConsumer.this.logger.info("Reject message:{},{},{}", new Object[]{messageExt.getMsgId(), messageExt.getTopic(), messageExt.getTags()});
                        return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                    }
                } catch (Exception var6) {
                    AbstractRocketMqConsumer.this.logger.warn("Consume message failed. messageExt:{},{},{},{}", new Object[]{messageExt.getMsgId(), messageExt.getTopic(), messageExt.getTags(), var6.getMessage()});
                    context.setDelayLevelWhenNextConsume(AbstractRocketMqConsumer.this.delayLevelWhenNextConsume);
                    return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                }
            } else {
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        }
    }
}

