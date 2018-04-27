package com.spring.akka.eventsourcing.example.events;

import java.io.Serializable;

import com.spring.akka.eventsourcing.example.OrderStatus;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@AllArgsConstructor
@NoArgsConstructor
public abstract class OrderEvent implements Serializable {
	private String orderId;
	private OrderStatus orderStatus;

}
