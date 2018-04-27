package com.spring.akka.eventsourcing.persistance.eventsourcing.actions;

import lombok.Builder;

/**
 * simply it mean nothing to be persisted as an event which is the case of readOnly command handlers
 */
@Builder
public class PersistNone extends Persist {

	public PersistNone() {
		super(null, null);
	}
}
