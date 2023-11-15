package com.cecloud.rocketmq.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;

/**
 * 用于接受消息处理
 */
@Component
@Slf4j
public class MessageListenerConcurrentlyImpl implements MessageListenerConcurrently {

    /**
     * 收到消息后，会回调此方法
     *
     * @param msgList   消息列表
     * @param context   上下文
     * @return  消费状态
     */
    @Override
    public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgList, ConsumeConcurrentlyContext context) {
        System.out.printf("%s Receive New Messages: %s %n", Thread.currentThread().getName(), msgList);
        for (MessageExt messageExt : msgList) {
            doSomeBusiness(messageExt);
        }
        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
    }

    /**
     * 业务消息处理
     *
     * @param messageExt    消息体
     */
    private void doSomeBusiness(MessageExt messageExt) {
        log.info("消费到订单消息：{}", Arrays.toString(messageExt.getBody()));
    }
}
