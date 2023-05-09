package cn.wolfcode;

import cn.wolfcode.util.OrderStep;
import cn.wolfcode.util.OrderUtil;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;


@SpringBootTest
public class RocketMQTest {
    @Autowired
    private RocketMQTemplate rocketMQTemplate;

    @Test
    public void sendMsg() {
        Message msg = MessageBuilder.withPayload("boot发送同步消息").build();
        rocketMQTemplate.send("helloTopicBoot", msg);
    }

    @Test
    public void sendSYNCMsg() throws InterruptedException {
        Message msg = MessageBuilder.withPayload("boot发送异步消息").build();
        rocketMQTemplate.asyncSend("helloTopicBoot", msg, new SendCallback() {
            @Override
            public void onSuccess(SendResult sendResult) {
                System.out.println("发送状态：" + sendResult.getSendStatus());
            }

            @Override
            public void onException(Throwable throwable) {
                System.out.println("消息发送失败");
            }
        });
        TimeUnit.SECONDS.sleep(5);
    }

    @Test
    public void sendOneMsg() {
        for (int i = 0; i < 10; i++) {
            Message<String> msg = MessageBuilder.withPayload("boot发送一次性消息" + i).build();
            rocketMQTemplate.sendOneWay("helloTopicBoot", msg);
        }
    }

    @Test
    public void sendOrderlyMsg() {
        //设置队列选择器
        rocketMQTemplate.setMessageQueueSelector(new MessageQueueSelector() {
            @Override
            public MessageQueue select(
                    List<MessageQueue> list,
                    org.apache.rocketmq.common.message.Message message, Object o) {
                String orderIdStr = (String) o;
                long orderId = Long.parseLong(orderIdStr);
                int index = (int) (orderId % list.size());
                return list.get(index);
            }
        });
        List<OrderStep> orderSteps = OrderUtil.buildOrders();
        for (OrderStep orderStep : orderSteps) {
            Message<String> msg = MessageBuilder.withPayload(orderStep.toString()).build();
            rocketMQTemplate.sendOneWayOrderly(
                    "orderlyTopicBoot", msg, String.valueOf(orderStep.getOrderId()));
        }
    }

    @Test
    public void sendDelayMsg() {
        Message<String> msg = MessageBuilder.withPayload("boot发送延时消息，发送时间：" + new Date()).build();
        rocketMQTemplate.syncSend("helloTopicBoot", msg, 3000, 3);
    }

    @Test
    public void sendTagFilterMsg() {
        Message<String> msg1 = MessageBuilder.withPayload("消息A").build();
        rocketMQTemplate.sendOneWay("tagFilterBoot:TagA", msg1);
        Message<String> msg2 = MessageBuilder.withPayload("消息B").build();
        rocketMQTemplate.sendOneWay("tagFilterBoot:TagB", msg2);
        Message<String> msg3 = MessageBuilder.withPayload("消息C").build();
        rocketMQTemplate.sendOneWay("tagFilterBoot:TagC", msg3);
    }

    @Test
    public void sendSQL92FilterMsg() {
        Message<String> msg1 = MessageBuilder.withPayload("美女A，年龄22，体重45")
                .setHeader("age", 22)
                .setHeader("weight", 45)
                .build();
        rocketMQTemplate.sendOneWay("SQL92FilterBoot", msg1);
        Message<String> msg2 = MessageBuilder.withPayload("美女B，年龄25，体重60")
                .setHeader("age", 25)
                .setHeader("weight", 60)
                .build();
        rocketMQTemplate.sendOneWay("SQL92FilterBoot", msg2);
        Message<String> msg3 = MessageBuilder.withPayload("美女C，年龄40，体重70")
                .setHeader("age", 40)
                .setHeader("weight", 70)
                .build();
        rocketMQTemplate.sendOneWay("SQL92FilterBoot", msg3);
    }
}

