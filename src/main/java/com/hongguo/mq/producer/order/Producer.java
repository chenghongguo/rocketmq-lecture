package com.hongguo.mq.producer.order;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;

import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

public class Producer {
    public static final String NAME_SERVER = "localhost:9876";

    public static void main(String[] args) throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("Producer-Topic-Sequence");
        producer.setNamesrvAddr(NAME_SERVER);
        producer.setSendMsgTimeout(10000);
        producer.start();

        String[] tags = new String[]{"TagA", "TagC", "TagD"};

        List<OrderStep> orders = OrderStep.buildOrders();

        Date date = new Date();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String dataStr = sdf.format(date);

        for (int i = 0; i < 10; i++) {
            String body = dataStr.concat(" Hello RocketMQ ") + orders.get(i);
            Message msg = new Message("Topic-Sequence-Test", tags[i % tags.length], "KEY" + i, body.getBytes(StandardCharsets.UTF_8));

            SendResult sendResult = producer.send(msg, new MessageQueueSelector() {
                @Override
                public MessageQueue select(List<MessageQueue> mqs, Message message, Object arg) {
                    Long id = (Long) arg;  //根据订单id选择发送queue
                    long index = id % mqs.size();
                    return mqs.get((int) index);
                }
            }, orders.get(i).getOrderId());

            System.out.printf("SendResult status:%s, queueId:%d, body:%s%n",
                    sendResult.getSendStatus(),
                    sendResult.getMessageQueue().getQueueId(),
                    body);
        }
        producer.shutdown();
    }
}
