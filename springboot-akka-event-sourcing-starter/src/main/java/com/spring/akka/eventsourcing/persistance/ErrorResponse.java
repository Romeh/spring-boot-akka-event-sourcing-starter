package com.spring.akka.eventsourcing.persistance;

import java.io.Serializable;

import lombok.Builder;
import lombok.Value;

/**
 * Generic Error response class
 */
@Builder
@Value
public class ErrorResponse implements Serializable {
	private String errorMsg;
	private String errorCode;
	private String exceptionMsg;
}
