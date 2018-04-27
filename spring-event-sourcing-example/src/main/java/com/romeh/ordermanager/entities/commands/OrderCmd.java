package com.romeh.ordermanager.entities.commands;

import java.io.Serializable;
import java.util.Map;

import lombok.Value;


public interface OrderCmd extends Serializable {

	String getOrderId();

	Map<String, String> getOrderDetails();

	@Value
	final class CreateCmd implements OrderCmd {
		private String orderId;
		private Map<String, String> orderDetails;
	}

	@Value
	final class ValidateCmd implements OrderCmd {
		private String orderId;
		private Map<String, String> orderDetails;
	}

	@Value
	final class SignCmd implements OrderCmd {
		private String orderId;
		private Map<String, String> orderDetails;
	}

	@Value
	final class GetOrderStatusCmd implements OrderCmd {
		private String orderId;
		private Map<String, String> orderDetails;
	}

	@Value
	final class AsyncSignCmd implements OrderCmd {
		private String orderId;
		private Map<String, String> orderDetails;
	}
}
