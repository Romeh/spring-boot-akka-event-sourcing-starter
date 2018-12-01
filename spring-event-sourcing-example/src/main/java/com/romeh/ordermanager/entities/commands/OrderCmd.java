package com.romeh.ordermanager.entities.commands;


import java.io.Serializable;
import java.util.Map;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import lombok.Value;


public interface OrderCmd extends Serializable {

	String getOrderId();

	Map<String, String> getOrderDetails();

	@Value
	@JsonDeserialize(as = CreateCmd.class)
	final class CreateCmd implements OrderCmd {
		private String orderId;
		private Map<String, String> orderDetails;
	}

	@Value
	@JsonDeserialize(as = ValidateCmd.class)
	final class ValidateCmd implements OrderCmd {
		private String orderId;
		private Map<String, String> orderDetails;
	}

	@Value
	@JsonDeserialize(as = SignCmd.class)
	final class SignCmd implements OrderCmd {
		private String orderId;
		private Map<String, String> orderDetails;
	}

	@Value
	@JsonDeserialize(as = GetOrderStatusCmd.class)
	final class GetOrderStatusCmd implements OrderCmd {
		private String orderId;
		private Map<String, String> orderDetails;
	}

	@Value
	@JsonDeserialize(as = AsyncSignCmd.class)
	final class AsyncSignCmd implements OrderCmd {
		private String orderId;
		private Map<String, String> orderDetails;
	}
}
