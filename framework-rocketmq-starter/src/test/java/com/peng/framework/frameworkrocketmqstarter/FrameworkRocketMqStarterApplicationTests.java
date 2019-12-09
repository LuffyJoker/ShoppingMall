package com.peng.framework.frameworkrocketmqstarter;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;

import java.util.Map;

@SpringBootTest
class FrameworkRocketMqStarterApplicationTests {

    @Test
    void contextLoads() {
    }

    @Autowired
    ApplicationContext applicationContext;

    @Test
    public void test() {
        //  获取Test注解的类
        Map<String, Object> beans = applicationContext.getBeansWithAnnotation(SpringBootApplication.class);
        for (Map.Entry<String, Object> entry : beans.entrySet()) {
            System.out.println("name:" + entry.getKey() + ",value:{}" + entry.getValue().getClass());
        }
    }
}
