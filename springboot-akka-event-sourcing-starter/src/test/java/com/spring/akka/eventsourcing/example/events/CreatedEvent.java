package com.spring.akka.eventsourcing.example.events;


import com.spring.akka.eventsourcing.example.OrderStatus;

import lombok.EqualsAndHashCode;
import lombok.Value;

@Value
@EqualsAndHashCode(callSuper = true)
public class CreatedEvent extends OrderEvent {

	public CreatedEvent(String orderId, OrderStatus orderStatus) {
		super(orderId, orderStatus);
	}

}
