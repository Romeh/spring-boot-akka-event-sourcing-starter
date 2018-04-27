package com.spring.akka.eventsourcing.example;

import java.io.Serializable;
import java.util.List;

import com.spring.akka.eventsourcing.example.events.OrderEvent;

import lombok.Value;

@Value
public class OrderState implements Serializable {

	private final List<OrderEvent> eventsHistory;
	private final OrderStatus orderStatus;

	public OrderState(List<OrderEvent> eventsHistory, OrderStatus orderStatus) {
		this.eventsHistory = eventsHistory;

		this.orderStatus = orderStatus;
	}


}
