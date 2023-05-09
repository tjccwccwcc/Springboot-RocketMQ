package cn.wolfcode.listener;

import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.stereotype.Component;

import java.nio.charset.Charset;

@Component
@RocketMQMessageListener(consumerGroup = "tagFilterGroupBoot",
        topic = "tagFilterBoot", selectorExpression = "TagA || TagC")
public class TagFilterTopicListener implements RocketMQListener<MessageExt> {
    @Override
    public void onMessage(MessageExt messageExt) {
        System.out.println("收到的消息：" + new String(messageExt.getBody(), Charset.defaultCharset()));
    }
}