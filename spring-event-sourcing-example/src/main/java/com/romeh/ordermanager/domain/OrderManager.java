package com.romeh.ordermanager.domain;

import static akka.actor.SupervisorStrategy.escalate;
import static akka.actor.SupervisorStrategy.restart;
import static akka.actor.SupervisorStrategy.resume;
import static akka.actor.SupervisorStrategy.stop;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Autowired;

import com.romeh.ordermanager.entities.OrderState;
import com.romeh.ordermanager.entities.Response;
import com.romeh.ordermanager.entities.commands.OrderCmd;
import com.romeh.ordermanager.entities.enums.OrderStatus;
import com.romeh.ordermanager.entities.events.CreatedEvent;
import com.romeh.ordermanager.entities.events.FinishedEvent;
import com.romeh.ordermanager.entities.events.OrderEvent;
import com.romeh.ordermanager.entities.events.SignedEvent;
import com.romeh.ordermanager.entities.events.ValidatedEvent;
import com.spring.akka.eventsourcing.config.PersistentEntityProperties;
import com.spring.akka.eventsourcing.persistance.AsyncResult;
import com.spring.akka.eventsourcing.persistance.eventsourcing.ExecutionFlow;
import com.spring.akka.eventsourcing.persistance.eventsourcing.FlowContext;
import com.spring.akka.eventsourcing.persistance.eventsourcing.PersistentEntity;
import com.spring.akka.eventsourcing.persistance.eventsourcing.ReadOnlyFlowContext;
import com.spring.akka.eventsourcing.persistance.eventsourcing.actions.Persist;
import com.spring.akka.eventsourcing.persistance.eventsourcing.annotations.PersistentActor;

import akka.actor.OneForOneStrategy;
import akka.actor.SupervisorStrategy;
import akka.japi.pf.DeciderBuilder;
import scala.concurrent.duration.Duration;

/**
 * Tha main Event sourcing DDD aggregate class for order domain which handle the order commands within it is boundary context
 *
 * @author romeh
 */
@PersistentActor
public class OrderManager extends PersistentEntity<OrderCmd, OrderEvent, OrderState> {

	/**
	 * how to handle supervisor strategy definition for the parent actor of the entity
	 */
	private static SupervisorStrategy strategy =
			new OneForOneStrategy(10, Duration.create(1, TimeUnit.MINUTES), DeciderBuilder.
					match(ArithmeticException.class, e -> resume()).
					match(NullPointerException.class, e -> restart()).
					match(IllegalArgumentException.class, e -> stop()).
					matchAny(o -> escalate()).build());

	/**
	 * @param persistentEntityConfig the akka persistent entity configuration
	 */
	@Autowired
	public OrderManager(PersistentEntityProperties<OrderManager, OrderCmd, OrderEvent> persistentEntityConfig) {
		super(persistentEntityConfig);
	}

	/**
	 * @param state the current State
	 * @return the initialized behavior for the entity
	 */
	@Override
	protected ExecutionFlow<OrderCmd, OrderEvent, OrderState> executionFlow(OrderState state) {
		switch (state.getOrderStatus()) {
			case NotStarted:
				return notStarted(state);
			case Created:
				return waitingForValidation(state);
			case Validated:
				return waitingForSigning(state);
			case Signed:
				return complected(state);
			case COMPLETED:
				return complected(state);
			default:
				throw new IllegalStateException();

		}
	}

	@Override
	protected OrderState initialState() {
		return new OrderState(Collections.emptyList(), OrderStatus.NotStarted);
	}

	/**
	 * ExecutionFlow for the not started state.
	 */
	private ExecutionFlow<OrderCmd, OrderEvent, OrderState> notStarted(OrderState state) {
		final ExecutionFlow.ExecutionFlowBuilder<OrderCmd, OrderEvent, OrderState> executionFlowBuilder = newFlowBuilder(state);

		// Command handlers
		executionFlowBuilder.onCommand(OrderCmd.CreateCmd.class, (start, ctx, currentState) ->
				persistAndReply(ctx, new CreatedEvent(start.getOrderId(), OrderStatus.Created))
		);

		// Event handlers
		executionFlowBuilder.onEvent(CreatedEvent.class, (started, currentState) ->
				createImmutableState(state, started, OrderStatus.Created)
		);

		return executionFlowBuilder.build();
	}

	/**
	 * ExecutionFlow for the not created and not yet validated.
	 */

	private ExecutionFlow<OrderCmd, OrderEvent, OrderState> waitingForValidation(OrderState state) {
		final ExecutionFlow.ExecutionFlowBuilder<OrderCmd, OrderEvent, OrderState> executionFlowBuilder = newFlowBuilder(state);
		// Command handlers
		executionFlowBuilder.onCommand(OrderCmd.ValidateCmd.class, (start, ctx, currentState) ->
				persistAndReply(ctx, new ValidatedEvent(start.getOrderId(), OrderStatus.Validated))
		);
		// Read only command handlers
		executionFlowBuilder.onReadOnlyCommand(OrderCmd.CreateCmd.class, this::alreadyDone);
		executionFlowBuilder.onReadOnlyCommand(OrderCmd.SignCmd.class, this::NotAllowed);
		executionFlowBuilder.onReadOnlyCommand(OrderCmd.GetOrderStatusCmd.class, (cmd, ctx) -> ctx.reply(getState()));

		// Event handlers
		executionFlowBuilder.onEvent(ValidatedEvent.class, (validated, currentState) ->
				createImmutableState(state, validated, validated.getOrderStatus())
		);

		return executionFlowBuilder.build();
	}

	/**
	 * ExecutionFlow for the not validated and not yet signed.
	 */
	private ExecutionFlow<OrderCmd, OrderEvent, OrderState> waitingForSigning(OrderState state) {
		final ExecutionFlow.ExecutionFlowBuilder<OrderCmd, OrderEvent, OrderState> executionFlowBuilder = newFlowBuilder(state);
		// Command handlers
		executionFlowBuilder.onCommand(OrderCmd.SignCmd.class, (start, ctx, currentState) ->
				persistAndReply(ctx, new SignedEvent(start.getOrderId(), OrderStatus.Signed))
		);
		// Async Command handler
		executionFlowBuilder.asyncOnCommand(OrderCmd.AsyncSignCmd.class, (signed, ctx, currentState) -> CompletableFuture
				.supplyAsync(() -> AsyncResult.<OrderEvent>builder()
						.persist(persistAndReply(ctx, new SignedEvent(signed.getOrderId(), OrderStatus.Signed)))
						.build())
		);
		// Read only command handlers
		executionFlowBuilder.onReadOnlyCommand(OrderCmd.GetOrderStatusCmd.class, (cmd, ctx) -> ctx.reply(getState()));
		executionFlowBuilder.onReadOnlyCommand(OrderCmd.ValidateCmd.class, this::alreadyDone);
		executionFlowBuilder.onReadOnlyCommand(OrderCmd.CreateCmd.class, this::alreadyDone);
		// Event handlers
		executionFlowBuilder.onEvent(SignedEvent.class, (signed, currentState) ->
				createImmutableState(state, signed, signed.getOrderStatus())
		);

		return executionFlowBuilder.build();
	}

	/**
	 * ExecutionFlow for signed and final state
	 */
	private ExecutionFlow<OrderCmd, OrderEvent, OrderState> complected(OrderState state) {
		final ExecutionFlow.ExecutionFlowBuilder<OrderCmd, OrderEvent, OrderState> executionFlowBuilder = newFlowBuilder(state);
		// just read only command handlers as it is final state
		executionFlowBuilder.onReadOnlyCommand(OrderCmd.GetOrderStatusCmd.class, (cmd, ctx) -> ctx.reply(getState()));
		executionFlowBuilder.onReadOnlyCommand(OrderCmd.CreateCmd.class, this::alreadyDone);
		executionFlowBuilder.onReadOnlyCommand(OrderCmd.ValidateCmd.class, this::alreadyDone);
		executionFlowBuilder.onReadOnlyCommand(OrderCmd.SignCmd.class, this::alreadyDone);
		// Event handlers
		executionFlowBuilder.onEvent(FinishedEvent.class, (finished, currentState) ->
				createImmutableState(state, finished, finished.getOrderStatus())
		);

		return executionFlowBuilder.build();
	}

	/**
	 * @param testState   current state
	 * @param testEvent   new event
	 * @param orderStatus new order status
	 * @return immutable state
	 */
	private OrderState createImmutableState(OrderState testState, OrderEvent testEvent, OrderStatus orderStatus) {
		final List<OrderEvent> eventsHistory = new ArrayList<>(testState.getEventsHistory());
		eventsHistory.add(testEvent);
		return new OrderState(eventsHistory, orderStatus);

	}

	/**
	 * Persist a single event then respond with done.
	 */
	private Persist<OrderEvent> persistAndDone(FlowContext ctx, OrderEvent event) {
		return ctx.thenPersist(event, (e) -> ctx.reply(Response.builder().orderId(event.getOrderId()).responseMsg("successfully executed").orderStatus(event.getOrderStatus().name()).build()));
	}

	/**
	 * Persist a single event then respond with done.
	 */
	private Persist<OrderEvent> persistAndReply(FlowContext ctx, OrderEvent event) {
		return ctx.thenPersist(event, (e) -> ctx.reply(Response.builder().orderStatus(event.getOrderStatus().name()).orderId(event.getOrderId()).build()));
	}

	/**
	 * Convenience method to handle when a command has already been processed (idempotent processing).
	 */
	private void alreadyDone(OrderCmd cmd, ReadOnlyFlowContext ctx) {
		ctx.reply(Response.builder().orderId(cmd.getOrderId()).responseMsg("the command is already done and applied before").build());
	}

	/**
	 * Convenience method to handle when a command has is not allowed based into order state.
	 */
	private void NotAllowed(OrderCmd cmd, ReadOnlyFlowContext ctx) {
		ctx.reply(Response.builder().orderId(cmd.getOrderId()).errorMessage("the request action is not allowed for the current order statue").errorCode("1111").build());
	}

	/**
	 * @return supervisorStrategy the actor supervisor strategy
	 */
	@Override
	public SupervisorStrategy supervisorStrategy() {
		return strategy;
	}

}
