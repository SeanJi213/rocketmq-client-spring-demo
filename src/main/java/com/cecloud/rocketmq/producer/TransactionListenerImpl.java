package com.cecloud.rocketmq.producer;

import com.alibaba.fastjson.JSON;
import com.cecloud.rocketmq.DTO.TransactionLog;
import com.cecloud.rocketmq.PubTools;
import com.cecloud.rocketmq.TransactionLogManager;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

/**
 * rocketmq事务的监听类
 */
@Component
@Slf4j
public class TransactionListenerImpl implements TransactionListener {

    @Resource
    private TransactionLogManager transactionLogManager;

    /**
     * 在这个方法中编写事务逻辑，建议使用try-catch块
     * {@link LocalTransactionState#COMMIT_MESSAGE} ： 当本地事务成功执行后返回此状态
     * {@link LocalTransactionState#ROLLBACK_MESSAGE} ： 当本地事务执行失败，需要回滚，那么返回此状态
     * {@link LocalTransactionState#UNKNOW} ：   当本地事务状态未知，或事务执行时间较长，无法判断其最终状态，
     *                                          那么此时可返回此状态，后续broker还会进行回调
     *
     * @param message   消息体
     * @param arg   参数，可忽略
     * @return  事务状态
     */
    @Override
    public LocalTransactionState executeLocalTransaction(Message message, Object arg) {
        LocalTransactionState localState;
        log.info("执行本地事务：");
        try {
            String transactionId = message.getTransactionId();
            String body = new String(message.getBody());
            localTransactionCreateOrder(transactionId, body);
            localState = LocalTransactionState.COMMIT_MESSAGE;
            log.info("已提交本地事务: {}", message.getTransactionId());
        } catch (Exception e) {
            log.error("本地事务执行失败: ", e);
            transactionRollBack(message);
            localState = LocalTransactionState.ROLLBACK_MESSAGE;
        }
        return localState;
    }


    /**
     * 事务异常，需要执行事务回滚或者事务补偿等操作
     *
     * @param msg   消息体
     */
    private void transactionRollBack(Message msg) {
        // 撤销订单
        transactionLogManager.remove(msg.getTransactionId());
        log.info("本地事务回滚，删除对应订单: {}", JSON.toJSONString(msg));
    }

    /**
     * 当某个事物操作在第一次不成功时，即事务状态不是{@link LocalTransactionState#COMMIT_MESSAGE}时，
     * Broker会在合适的时机多次回调此方法
     *
     * @param msg   消息体
     * @return  事务状态
     */
    @Override
    public LocalTransactionState checkLocalTransaction(MessageExt msg) {
        System.out.println(PubTools.now() + " ::: " + msg.getKeys() + ", checkLocalTransaction ");
        log.info("回查本地事务状态: {}", msg.getTransactionId());
        LocalTransactionState state;
        String transactionId = msg.getTransactionId();
        if (transactionLogManager.contains(transactionId)) {
            state = LocalTransactionState.COMMIT_MESSAGE;
        } else {
            state = LocalTransactionState.UNKNOW;
        }
        log.info("返回本地事务状态为: {}", state);
        return state;
    }

    public void localTransactionCreateOrder(String transactionId, String body) {
        // 把本地事务存入日志
        TransactionLog transactionLog = new TransactionLog();
        transactionLog.setId(transactionId);
        transactionLog.setDetail(body);
        transactionLogManager.insert(transactionLog);
        log.info("创建订单成功，订单: {}", JSON.toJSONString(transactionLog));
    }
}
