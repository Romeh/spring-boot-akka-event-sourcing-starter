package com.spring.akka.eventsourcing.example.events;

import com.spring.akka.eventsourcing.example.OrderStatus;

import lombok.EqualsAndHashCode;
import lombok.Value;

@EqualsAndHashCode(callSuper = true)
@Value
public class ValidatedEvent extends OrderEvent {

	public ValidatedEvent(String orderId, OrderStatus orderStatus) {
		super(orderId, orderStatus);
	}

}