package com.romeh.ordermanager.entities;


import lombok.Builder;
import lombok.Data;

/**
 * generic service response object
 */
@Data
@Builder
public class Response {

	private String orderId;
	private String orderStatus;
	private String errorCode;
	private String responseMsg;
	private String errorMessage;

}
