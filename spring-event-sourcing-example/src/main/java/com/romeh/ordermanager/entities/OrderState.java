package com.romeh.ordermanager.entities;

import java.io.Serializable;
import java.util.List;

import com.romeh.ordermanager.entities.enums.OrderStatus;
import com.romeh.ordermanager.entities.events.OrderEvent;

import lombok.Value;

/**
 * the main immutable order state object
 */
@Value
public class OrderState implements Serializable {

	private final List<OrderEvent> eventsHistory;
	private final OrderStatus orderStatus;

	public OrderState(List<OrderEvent> eventsHistory, OrderStatus orderStatus) {
		this.eventsHistory = eventsHistory;

		this.orderStatus = orderStatus;
	}


}
