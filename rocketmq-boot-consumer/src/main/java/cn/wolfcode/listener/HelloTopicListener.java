package cn.wolfcode.listener;

import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.spring.annotation.MessageModel;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.stereotype.Component;

import java.nio.charset.Charset;
import java.util.Date;

@Component
//messageModel = MessageModel.BROADCASTING设置广播
@RocketMQMessageListener(consumerGroup = "htbConsumerGroup", topic = "helloTopicBoot", messageModel = MessageModel.BROADCASTING)
public class HelloTopicListener implements RocketMQListener<MessageExt> {
    @Override
    public void onMessage(MessageExt messageExt) {
        System.out.println("消费的时间：" + new Date() + "，收到的消息："
                + new String(messageExt.getBody(), Charset.defaultCharset()));
    }
}
