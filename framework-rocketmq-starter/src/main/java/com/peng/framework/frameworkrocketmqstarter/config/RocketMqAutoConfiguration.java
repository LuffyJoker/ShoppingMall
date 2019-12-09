package com.peng.framework.frameworkrocketmqstarter.config;

import com.peng.framework.frameworkrocketmqstarter.common.AbstractRocketMqConsumer;
import com.peng.framework.frameworkrocketmqstarter.common.DefaultRocketMqProducer;
import com.peng.framework.frameworkrocketmqstarter.common.RocketMqConsumerMBean;
import com.peng.framework.frameworkrocketmqstarter.util.RunTimeUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;
import org.springframework.util.CollectionUtils;

import javax.annotation.Resource;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Configuration
@ConditionalOnClass({DefaultMQPushConsumer.class})
@EnableConfigurationProperties({RocketMqProperties.class})
public class RocketMqAutoConfiguration {
    private static final Logger LOGGER = LoggerFactory.getLogger(RocketMqAutoConfiguration.class);
    @Resource
    private RocketMqProperties rocketMqProperties;

    public RocketMqAutoConfiguration() {
    }

    @Bean
    @ConditionalOnClass({DefaultMQProducer.class})
    @ConditionalOnMissingBean({DefaultMQProducer.class})
    public DefaultMQProducer mqProducer() {
        DefaultMQProducer producer = new DefaultMQProducer();
        producer.setProducerGroup(this.rocketMqProperties.getProducerGroupName());
        producer.setNamesrvAddr(this.rocketMqProperties.getNamesrvAddr());
        producer.setSendMsgTimeout(this.rocketMqProperties.getSendMsgTimeout());
        producer.setRetryTimesWhenSendFailed(this.rocketMqProperties.getRetryTimesWhenSendFailed());
        producer.setRetryTimesWhenSendAsyncFailed(this.rocketMqProperties.getRetryTimesWhenSendAsyncFailed());
        producer.setMaxMessageSize(this.rocketMqProperties.getMaxMessageSize());
        producer.setCompressMsgBodyOverHowmuch(this.rocketMqProperties.getCompressMsgBodyOverHowMuch());
        producer.setRetryAnotherBrokerWhenNotStoreOK(this.rocketMqProperties.isRetryAnotherBrokerWhenNotStoreOk());
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOGGER.info("Producer shutdown");
            producer.shutdown();
        }));

        try {
            producer.start();
            LOGGER.info("RocketMQ Producer Started, NamesrvAddr:{}, Group:{}", this.rocketMqProperties.getNamesrvAddr(), this.rocketMqProperties.getProducerGroupName());
        } catch (MQClientException var3) {
            LOGGER.error("Producer Start ERROR, NamesrvAddr:{}, Group:{}", new Object[]{this.rocketMqProperties.getNamesrvAddr(), this.rocketMqProperties.getProducerGroupName(), var3});
        }

        return producer;
    }

    @Bean(
            destroyMethod = "destroy"
    )
    @ConditionalOnBean({DefaultMQProducer.class})
    @ConditionalOnMissingBean(
            name = {"defaultRocketMqProducer"}
    )
    public DefaultRocketMqProducer defaultRocketMqProducer(@Qualifier("mqProducer") DefaultMQProducer mqProducer) {
        DefaultRocketMqProducer defaultRocketMqProducer = new DefaultRocketMqProducer();
        defaultRocketMqProducer.setProducer(mqProducer);
        return defaultRocketMqProducer;
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnBean({AbstractRocketMqConsumer.class})
    @Order
    public RocketMqConsumerMBean rocketMqConsumerMBean(List<AbstractRocketMqConsumer> messageListeners) {
        RocketMqConsumerMBean rocketMqConsumerMBean = new RocketMqConsumerMBean();
        messageListeners.forEach(this::registerMQConsumer);
        rocketMqConsumerMBean.setConsumers(messageListeners);
        return rocketMqConsumerMBean;
    }

    private void registerMQConsumer(AbstractRocketMqConsumer rocketMqConsumer) {
        Map<String, Set<String>> subscribeTopicTags = rocketMqConsumer.subscribeTopicTags((Map) null);
        DefaultMQPushConsumer mqPushConsumer = rocketMqConsumer.getConsumer();
        mqPushConsumer.setInstanceName(RunTimeUtil.getRocketMqUniqeInstanceName());
        mqPushConsumer.setNamesrvAddr(this.rocketMqProperties.getNamesrvAddr());
        subscribeTopicTags.entrySet().forEach((e) -> {
            try {
                String rocketMqTopic = (String) e.getKey();
                Set<String> rocketMqTags = (Set) e.getValue();
                if (CollectionUtils.isEmpty(rocketMqTags)) {
                    mqPushConsumer.subscribe(rocketMqTopic, "*");
                } else {
                    String tags = StringUtils.join(rocketMqTags, " || ");
                    mqPushConsumer.subscribe(rocketMqTopic, tags);
                    LOGGER.info("Subscribe, Topic:{}, Tags:{}", rocketMqTopic, tags);
                }
            } catch (MQClientException var5) {
                LOGGER.error("Consumer Subscribe error", var5);
            }

        });
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOGGER.info("Consumer shutdown");
            mqPushConsumer.shutdown();
        }));

        try {
            mqPushConsumer.start();
            rocketMqConsumer.setStarted(true);
            LOGGER.info("RocketMQ Consumer Started, NamesrvAddr:{}, Group:{}", this.rocketMqProperties.getNamesrvAddr(), rocketMqConsumer.getConsumerGroup());
        } catch (MQClientException var5) {
            LOGGER.error("Consumer start error, NamesrvAddr:{}, Group:{}", new Object[]{this.rocketMqProperties.getNamesrvAddr(), rocketMqConsumer.getConsumerGroup(), var5});
        }

    }
}

