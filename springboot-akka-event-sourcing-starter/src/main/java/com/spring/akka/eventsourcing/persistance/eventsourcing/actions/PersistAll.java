package com.spring.akka.eventsourcing.persistance.eventsourcing.actions;


import java.util.List;
import java.util.function.Consumer;

import lombok.Builder;
import lombok.Getter;

/**
 * main class for the action result that need to be done from the command context after executing the command handler in case of many events to persist
 *
 * @param <E> the event type
 */
public class PersistAll<E> extends Persist<E> {
	@Getter
	private List<E> events;
	@Getter
	private Consumer<List<E>> afterPersistAll;

	@Builder
	private PersistAll(Consumer<List<E>> afterPersist, List<E> events) {
		super(null, null);
		this.events = events;
		this.afterPersistAll = afterPersist;
	}
}
