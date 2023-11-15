package com.cecloud.rocketmq.producer;

import com.alibaba.fastjson.JSON;
import com.cecloud.rocketmq.PubTools;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.concurrent.*;

@Component
@Slf4j
public class ProducerFactory {
    private static volatile TransactionMQProducer transactionProducer = null;

    private static volatile DefaultMQProducer defaultMQProducer = null;

    // 执行本地事务和本地事务回查的监听器
    @Autowired
    private TransactionListenerImpl transactionListener;

    // 执行任务的线程池
    // 事务的check线程池，当调用回调方法 #initCommonMQProducer 时，将会使用此线程池
    // 一般情况下，如果业务事务一次性可以执行成功，那么不会走到check方法
    // 回调线程池可根据业务场景，自行配置其大小
    private final ExecutorService executorService = new ThreadPoolExecutor(10, 20, 60, TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(2000), r -> {
        Thread thread = new Thread(r);
        thread.setName("client-transaction-msg-check-thread");
        return thread;}
    );

    /**
     * 构造一个事务类型的Producer并返回
     *
     * @return  事务类型
     */
    private TransactionMQProducer initTransactionMQProducer() {
        try {
            TransactionMQProducer producer = new TransactionMQProducer("please_rename_unique_group_name");
            producer.setNamesrvAddr("localhost:9876");
            producer.setExecutorService(executorService);
            producer.setTransactionListener(transactionListener);
            producer.start();
            return producer;
        } catch (Exception e) {
            log.error("transactional message producer initialization failed for some reason!");
            throw new RuntimeException(e);
        }
    }

    /**
     * 构造一个普通类型的Producer并返回
     *
     * @return  普通类型
     */
    private DefaultMQProducer initCommonMQProducer() {
        try {
            DefaultMQProducer producer = new DefaultMQProducer("please_rename_unique_group_name");
            producer.setNamesrvAddr("localhost:9876");
            producer.start();
            return producer;
        } catch (Exception e) {
            log.error("default message producer failed initialization for some reason!");
            throw new RuntimeException(e);
        }
    }

    /**
     * 单例，获取producer引用
     */
    public TransactionMQProducer getTransactionProducer() {
        if (transactionProducer == null) {
            synchronized (ProducerFactory.class) {
                if (transactionProducer == null) {
                    transactionProducer = initTransactionMQProducer();
                }
            }
        }
        return transactionProducer;
    }

    public DefaultMQProducer getCommonProducer() {
        if (defaultMQProducer == null) {
            synchronized (ProducerFactory.class) {
                if (defaultMQProducer == null) {
                    defaultMQProducer = initCommonMQProducer();
                }
            }
        }
        return defaultMQProducer;
    }

    /**
     * 发送事务消息的demo，这里相对比较简单，只是触发了一下事务消息的发送，主要事务逻辑需要参考{@link TransactionListenerImpl}
     *
     * @throws Exception    各类异常
     */
    public void sendTransactionMsg() throws Exception {
        for (int i = 0; i < 10; i++) {
            Message msg = initMsg(i);
            System.out.println(PubTools.now() + " ::: " + msg.getKeys() + ", prepare send");
            SendResult sendResult = transactionProducer.sendMessageInTransaction(msg, null);
            System.out.println(PubTools.now() + " ::: " + msg.getKeys() + ", result is " + sendResult);
        }
    }

    /**
     * 普通消息的发送，如果当前消息不是事务消息，那么参照此方法代码
     *
     * @throws Exception    各类异常
     */
    public void sendCommonMsg() throws Exception {
        for (int i = 0; i < 10; i++) {
            Message msg = initMsg(i);
            SendResult sendResult = defaultMQProducer.send(msg);
            System.out.println(JSON.toJSONString(sendResult));
        }
    }

    private Message initMsg(int index) throws Exception {
        return new Message("TopicTest", "", "key" + index,
                ("Hello RocketMQ " + index).getBytes(RemotingHelper.DEFAULT_CHARSET));
    }


}
