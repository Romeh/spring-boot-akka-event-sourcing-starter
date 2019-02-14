package com.spring.akka.eventsourcing.persistance.eventsourcing.actions;

import java.io.Serializable;
import java.util.function.Consumer;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;


/**
 * main class for the action result that need to be done from the command context after executing the command handler
 *
 * @param <E> the event type
 */
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
public class Persist<E> implements Serializable {

	private E event;
	private Consumer<E> afterPersist;
}
