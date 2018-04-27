package com.spring.akka.eventsourcing.persistance;

import java.io.Serializable;

import com.spring.akka.eventsourcing.persistance.eventsourcing.actions.Persist;

import lombok.Builder;
import lombok.Value;

/**
 * @param <E> the event type
 * 
 *  the async result class that contain the async persist action and if exist the error response
 */
@Builder
@Value
public class AsyncResult<E> implements Serializable {
	private Persist<E> persist;
	private ErrorResponse errorResponse;

}
