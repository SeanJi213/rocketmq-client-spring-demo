package com.cecloud.rocketmq.Controller;


import com.cecloud.rocketmq.DTO.OrderDTO;
import com.cecloud.rocketmq.Service.OrderService;
import com.cecloud.rocketmq.TransactionLogManager;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/order")
@Slf4j
public class OrderController {

    @Autowired
    private TransactionLogManager transactionLogManager;

    @Autowired
    private OrderService orderService;

    @GetMapping("/list")
    public String listOrders() {
        return transactionLogManager.list();
    }

    @PostMapping("/create")
    public void createOrder(@RequestBody OrderDTO orderDTO) throws Exception {
        log.info("receive order data, transaction id={}",orderDTO.getTransactionId());
        orderService.createOrder(orderDTO);
    }
}
