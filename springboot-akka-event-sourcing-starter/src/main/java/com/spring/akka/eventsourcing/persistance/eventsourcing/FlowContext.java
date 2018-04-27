package com.spring.akka.eventsourcing.persistance.eventsourcing;

import java.util.List;
import java.util.function.Consumer;

import com.spring.akka.eventsourcing.persistance.eventsourcing.actions.Persist;
import com.spring.akka.eventsourcing.persistance.eventsourcing.actions.PersistAll;
import com.spring.akka.eventsourcing.persistance.eventsourcing.actions.PersistNone;
import com.spring.akka.eventsourcing.persistance.eventsourcing.actions.PersistOne;

public abstract class FlowContext extends ReadOnlyFlowContext {
	/**
	 * A command handler may return this `Persist` type to define
	 * that one event is to be persisted.
	 */
	public <E> Persist thenPersist(E event) {
		return PersistOne.builder().event(event).afterPersist(null).build();
	}

	/**
	 * A command handler may return this `Persist` type to define
	 * that one event is to be persisted. External side effects can be
	 * performed after successful persist in the `afterPersist` function.
	 */
	public <E> Persist<E> thenPersist(E event, Consumer<E> afterPersist) {

		return PersistOne.<E>builder().event(event).afterPersist(afterPersist).build();
	}

	/**
	 * A command handler may return this `Persist`type to define
	 * that several events are to be persisted.
	 */
	public <E> Persist thenPersistAll(List<E> events) {
		return PersistAll.<E>builder().afterPersist(null).events(events).build();
	}

	/**
	 * A command handler may return this `Persist` type to define
	 * that several events are to be persisted. External side effects can be
	 * performed after successful persist in the `afterPersist` function.
	 * `afterPersist` is invoked once when all events have been persisted
	 * successfully.
	 */
	public <E> Persist thenPersistAll(List<E> events, Consumer<List<E>> afterPersist) {
		return PersistAll.<E>builder().afterPersist(afterPersist).events(events).build();
	}


	/**
	 * A command handler may return this `Persist` type to define
	 * that no events are to be persisted.
	 */
	public <E> Persist done() {
		return PersistNone.<E>builder().build();
	}
}
