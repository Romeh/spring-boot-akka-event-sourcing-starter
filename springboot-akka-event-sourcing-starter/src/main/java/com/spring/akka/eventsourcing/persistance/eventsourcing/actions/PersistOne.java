package com.spring.akka.eventsourcing.persistance.eventsourcing.actions;

import java.util.function.Consumer;

import lombok.Builder;

/**
 * persist one event as a result of command handler execution and check if there is any after persist logic is needed.
 *
 * @param <E> the event type
 */

public class PersistOne<E> extends Persist {
	@Builder
	private PersistOne(E event, Consumer<E> afterPersist) {
		super(event, afterPersist);
	}
}
