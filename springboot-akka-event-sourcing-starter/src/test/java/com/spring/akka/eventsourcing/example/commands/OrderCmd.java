package com.spring.akka.eventsourcing.example.commands;

import java.io.Serializable;

import lombok.Value;


public interface OrderCmd extends Serializable {

	String getOrderId();

	@Value
	final class CreateCmd implements OrderCmd {
		private String orderId;
	}

	@Value
	final class ValidateCmd implements OrderCmd {
		private String orderId;
	}

	@Value
	final class SignCmd implements OrderCmd {
		private String orderId;
	}

	@Value
	final class GetOrderStatusCmd implements OrderCmd {
		private String orderId;
	}

	@Value
	final class AsyncSignCmd implements OrderCmd {
		private String orderId;
	}
}
