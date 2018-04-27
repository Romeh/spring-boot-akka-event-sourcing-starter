package com.spring.akka.eventsourcing.persistance.eventsourcing;

import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

import com.spring.akka.eventsourcing.persistance.AsyncResult;
import com.spring.akka.eventsourcing.persistance.eventsourcing.actions.Persist;

import lombok.Builder;
import lombok.Data;
import lombok.Singular;

/**
 * @param <C> root command class type
 * @param <E> root event class
 * @param <S> root state class
 *
 * builder class for the execution flow of the aggregate entity
 */
@Builder
@Data
public class ExecutionFlow<C, E, S> {

	private S state;
	@Singular("onCommand")
	private Map<Class<? extends C>, TriFunction<C, FlowContext, S, Persist<E>>> onCommand;
	@Singular("asyncOnCommand")
	private Map<Class<? extends C>, TriFunction<C, FlowContext, S, CompletionStage<AsyncResult<E>>>> asyncOnCommand;
	@Singular("onEvent")
	private Map<Class<? extends E>, BiFunction<E, S, S>> onEvent;


	public static class ExecutionFlowBuilder<C, E, S> {

		/*
		 * Register a read-only command handler for a given command class. A read-only command
		 * handler does not persist events (i.e. it does not change state) but it may perform side
		 * effects, such as replying to the request. Replies are sent with the `reply` method of the
		 * context that is passed to the command handler function.
		 */
		public ExecutionFlowBuilder<C, E, S> onReadOnlyCommand(Class<? extends C> command, BiConsumer<C, ReadOnlyFlowContext> handler) {

			onCommand(command, (cmd, flowContext, state) -> {
				handler.accept(cmd, flowContext);
				return flowContext.done();
			});
			return this;
		}


	}


}
