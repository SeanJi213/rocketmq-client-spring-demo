package com.cecloud.rocketmq.DTO;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class OrderDTO {
    private String transactionId;
    private String orderBody;
}
