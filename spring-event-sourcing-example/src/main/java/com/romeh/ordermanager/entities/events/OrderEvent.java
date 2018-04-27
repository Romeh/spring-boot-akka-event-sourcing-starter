package com.romeh.ordermanager.entities.events;

import java.io.Serializable;

import com.romeh.ordermanager.entities.enums.OrderStatus;

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
