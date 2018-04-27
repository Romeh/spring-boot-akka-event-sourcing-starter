package com.romeh.ordermanager.entities.events;


import com.romeh.ordermanager.entities.enums.OrderStatus;

import lombok.EqualsAndHashCode;
import lombok.Value;

@Value
@EqualsAndHashCode(callSuper = true)
public class CreatedEvent extends OrderEvent {

	public CreatedEvent(String orderId, OrderStatus orderStatus) {
		super(orderId, orderStatus);
	}

}
