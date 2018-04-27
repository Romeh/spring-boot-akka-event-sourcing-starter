package com.spring.akka.eventsourcing.example.events;


import com.spring.akka.eventsourcing.example.OrderStatus;

import lombok.EqualsAndHashCode;
import lombok.Value;

@Value
@EqualsAndHashCode(callSuper = true)
public class FinishedEvent extends OrderEvent {

	public FinishedEvent(String orderId, OrderStatus orderStatus) {
		super(orderId, orderStatus);
	}

}
