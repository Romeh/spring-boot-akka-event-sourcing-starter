package com.spring.akka.eventsourcing.persistance.eventsourcing;

public abstract class ReadOnlyFlowContext {

	/**
	 * Send reply to a command. The type `R` must be the type defined by
	 * the command.
	 */
	public abstract <R> void reply(R msg);

}
