package com.hongguo.mq.producer;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

public class SyncProducer {
    public static final String NAME_SERVER = "localhost:9876";

    public static void main(String[] args) throws Exception {
        // 实例化消息生产者Producer
        DefaultMQProducer producer = new DefaultMQProducer("Producer-Group-Test");
        // 设置NameServer地址
        producer.setNamesrvAddr(NAME_SERVER);
        // 启动Producer实例
        producer.start();
        // 设置发送超时时间
        producer.setSendMsgTimeout(10000);

        for (int i = 0; i < 10; i++) {
            // 创建消息，并指定Topic，Tag和消息体
            Message message = new Message("Topic-Test", "TagA", ("Hello RocketMQ " + i).getBytes(RemotingHelper.DEFAULT_CHARSET));
            // 发送消息到一个Broker
            SendResult sendResult = producer.send(message);
            // 通过sendResult返回消息是否发送成功
            System.out.println("sendResult:" + sendResult);
        }
        // 关闭Producer实例
        producer.shutdown();
    }
}
