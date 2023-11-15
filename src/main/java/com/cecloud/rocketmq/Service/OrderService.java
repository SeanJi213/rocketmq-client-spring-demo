package com.cecloud.rocketmq.Service;

import com.cecloud.rocketmq.DTO.OrderDTO;
import com.cecloud.rocketmq.PubTools;
import com.cecloud.rocketmq.producer.ProducerFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomUtils;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;

@Service
@Slf4j
public class OrderService {

    @Resource
    private ProducerFactory producerFactory;

    public void createOrder(OrderDTO orderDTO) throws MQClientException {
        Message message = new Message();
        message.setTransactionId(String.valueOf(RandomUtils.nextInt()));
        message.setBody(orderDTO.getOrderBody().getBytes());
        // 发送事务消息
        TransactionMQProducer transactionProducer = producerFactory.getTransactionProducer();
        log.info(PubTools.now() + " ::: " + message.getTransactionId() + ", prepare send");
        SendResult sendResult = transactionProducer.sendMessageInTransaction(message, null);
        log.info(PubTools.now() + " ::: " + message.getTransactionId() + ", result is " + sendResult);
    }
}
