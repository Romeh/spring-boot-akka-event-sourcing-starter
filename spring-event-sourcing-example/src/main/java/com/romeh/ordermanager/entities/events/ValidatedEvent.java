package com.romeh.ordermanager.entities.events;


import com.romeh.ordermanager.entities.enums.OrderStatus;

import lombok.EqualsAndHashCode;
import lombok.Value;

@EqualsAndHashCode(callSuper = true)
@Value
public class ValidatedEvent extends OrderEvent {

	public ValidatedEvent(String orderId, OrderStatus orderStatus) {
		super(orderId, orderStatus);
	}

}