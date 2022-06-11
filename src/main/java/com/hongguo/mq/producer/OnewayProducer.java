package com.hongguo.mq.producer;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;

import java.nio.charset.StandardCharsets;

public class OnewayProducer {
    public static final String NAME_SERVER = "localhost:9876";

    public static void main(String[] args) throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("Producer-Group-Oneway-Test");
        producer.setNamesrvAddr(NAME_SERVER);

        producer.start();
        producer.setSendMsgTimeout(10000);

        for (int i = 0; i < 10; i++) {
            Message message = new Message("Topic-Test-Oneway", "TagA", ("hello world " + i).getBytes(StandardCharsets.UTF_8));
            producer.sendOneway(message);
        }
        producer.shutdown();
    }
}
