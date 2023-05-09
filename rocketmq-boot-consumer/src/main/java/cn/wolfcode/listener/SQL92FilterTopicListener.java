package cn.wolfcode.listener;

import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.annotation.SelectorType;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.stereotype.Component;

import java.nio.charset.Charset;

@Component
@RocketMQMessageListener(consumerGroup = "SQL92FilterGroupBoot",
        topic = "SQL92FilterBoot", selectorType = SelectorType.SQL92,
        selectorExpression = "age > 23 and weight > 60")
public class SQL92FilterTopicListener implements RocketMQListener<MessageExt> {
    @Override
    public void onMessage(MessageExt messageExt) {
        System.out.println("收到的消息：" + new String(messageExt.getBody(), Charset.defaultCharset()));
    }
}