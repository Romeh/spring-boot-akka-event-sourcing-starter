package com.romeh.ordermanager.entities.events;


import com.romeh.ordermanager.entities.enums.OrderStatus;

import lombok.EqualsAndHashCode;
import lombok.Value;

@Value
@EqualsAndHashCode(callSuper = true)
public class FinishedEvent extends OrderEvent {

	public FinishedEvent(String orderId, OrderStatus orderStatus) {
		super(orderId, orderStatus);
	}

}
