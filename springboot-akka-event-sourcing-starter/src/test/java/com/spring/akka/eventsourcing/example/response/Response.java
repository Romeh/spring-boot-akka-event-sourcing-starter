package com.spring.akka.eventsourcing.example.response;


import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class Response {

	private String orderId;
	private String orderStatus;
	private int errorCode;
	private String errorMessage;
}
