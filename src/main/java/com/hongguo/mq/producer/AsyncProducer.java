package com.hongguo.mq.producer;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.CountDownLatch2;
import org.apache.rocketmq.common.message.Message;

import java.nio.charset.StandardCharsets;

public class AsyncProducer {
    public static final String NAME_SERVER = "localhost:9876";

    public static void main(String[] args) throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("Producer-Group-Async-Test");
        producer.setNamesrvAddr(NAME_SERVER);
        producer.start();
        producer.setSendMsgTimeout(10000);
        producer.setRetryTimesWhenSendAsyncFailed(0);

        int count = 10;
        final CountDownLatch2 countDownLatch = new CountDownLatch2(count);
        for (int i = 0; i < count; i++) {
            final int index = i;
            Message message = new Message("TopicTest", "TagA", "OrderID188", "hello world".getBytes(StandardCharsets.UTF_8));

            producer.send(message, new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    countDownLatch.countDown();
                    System.out.printf("%-10d OK %s %n", index,
                            sendResult.getMsgId());
                }

                @Override
                public void onException(Throwable e) {
                    countDownLatch.countDown();
                    System.out.printf("%-10d Exception %s %n", index, e);
                    e.printStackTrace();
                }
            });
        }

        countDownLatch.await();
        producer.shutdown();
    }
}
