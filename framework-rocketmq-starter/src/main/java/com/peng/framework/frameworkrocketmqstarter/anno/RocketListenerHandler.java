package com.peng.framework.frameworkrocketmqstarter.anno;

import com.peng.framework.frameworkrocketmqstarter.constants.ConsumeMode;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
public @interface RocketListenerHandler {

    MessageModel type() default MessageModel.CLUSTERING;

    ConsumeFromWhere consumerFromWhere() default ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET;

    int consumeThreadMin() default 10;

    int consumeThreadMax() default 32;

    ConsumeMode consumeMode() default ConsumeMode.CONCURRENTLY;

    int consumeMsgMaxSize() default 1;

    int reTryConsume() default 16;

    String consumerGroup();

    String topic();

    String[] tags();

    String messageSelector() default "";

    boolean reWriteSubscribe() default false;
}
