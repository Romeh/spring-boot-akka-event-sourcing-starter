package com.spring.akka.eventsourcing.persistance.eventsourcing;

import static akka.pattern.PatternsCS.pipe;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.springframework.util.StringUtils;

import com.spring.akka.eventsourcing.config.PersistentEntityProperties;
import com.spring.akka.eventsourcing.persistance.AsyncResult;
import com.spring.akka.eventsourcing.persistance.ErrorResponse;
import com.spring.akka.eventsourcing.persistance.ResumeProcessing;
import com.spring.akka.eventsourcing.persistance.eventsourcing.actions.Persist;
import com.spring.akka.eventsourcing.persistance.eventsourcing.actions.PersistAll;
import com.spring.akka.eventsourcing.persistance.eventsourcing.actions.PersistNone;
import com.spring.akka.eventsourcing.persistance.eventsourcing.actions.PersistOne;
import com.typesafe.config.ConfigValueFactory;

import akka.actor.ActorRef;
import akka.actor.ReceiveTimeout;
import akka.cluster.sharding.ShardRegion;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.japi.pf.ReceiveBuilder;
import akka.persistence.AbstractPersistentActor;
import akka.persistence.RecoveryCompleted;
import akka.persistence.SnapshotOffer;
import akka.persistence.journal.Tagged;
import io.vavr.collection.Vector;
import lombok.Getter;
import scala.PartialFunction;
import scala.concurrent.duration.Duration;
import scala.runtime.BoxedUnit;

public abstract class PersistentEntity<C, E, S> extends AbstractPersistentActor {

	/**
	 * stop message tha can be send to stop the entity
	 */
	private static final Stop STOP = new Stop();
	protected final LoggingAdapter log = Logging.getLogger(context().system(), this);
	private final int snapshotAfter;
	private final Map<Class<? extends E>, String> tags;
	private final Class<C> commandType;
	private final Class<E> eventType;
	private final String asyncDispatcherName;
	private final long asyncTimeout;
	@Getter
	private final String persistenceIdPrefix;
	@Getter
	private S state = initialState();
	private long eventCount = 0L;
	/**
	 * the idle waiting behavior for asynchronous command handler
	 */
	private final PartialFunction<Object, BoxedUnit> waitingForAsyncAction =
			ReceiveBuilder.create()
					.match(AsyncResult.class, asyncResult -> {
						if (asyncResult.getPersist() != null && asyncResult.getErrorResponse() == null) {
							Persist<E> nextAction = asyncResult.getPersist();
							handlePostCommandPersistAction(nextAction);
							context().unbecome();
							unstashAll();

						} else if (asyncResult.getErrorResponse() != null) {
							log.error("error response received {} , {} ,{} ",
									asyncResult.getErrorResponse().getErrorCode(),
									asyncResult.getErrorResponse().getErrorMsg(),
									asyncResult.getErrorResponse().getExceptionMsg());
							self().tell(asyncResult.getErrorResponse(), sender());
							context().unbecome();
							unstashAll();
						} else {
							log.error("un-handled async result {}", asyncResult.toString());
							unhandled(asyncResult);
							context().unbecome();
							unstashAll();
						}
					})
					.match(ResumeProcessing.class, resumeProcessing -> {
						context().unbecome();
						unstashAll();
					})
					.matchAny(any -> stash())
					.build().onMessage();

	/**
	 * @param persistentEntityConfig persistent entity configuration
	 */
	public PersistentEntity(PersistentEntityProperties<? extends PersistentEntity, C, E> persistentEntityConfig) {
		this.snapshotAfter = persistentEntityConfig.snapshotStateAfter();
		this.commandType = persistentEntityConfig.getRootCommandType();
		this.persistenceIdPrefix = persistentEntityConfig.persistenceIdPrefix();
		// get the async configured dispatcher name if exist
		this.asyncDispatcherName = !StringUtils.isEmpty(persistentEntityConfig.pipeDispatcherName()) ?
				persistentEntityConfig.asyncPersistentEntityDispatcherName() : "internal.blocking-io-dispatcher";
		// init the tag map names - event class to tag mapping
		this.tags = persistentEntityConfig.tags();
		this.eventType = persistentEntityConfig.getRootEventType();
		this.asyncTimeout = persistentEntityConfig.scheduledAsyncEntityActionTimeout() != 0 ? persistentEntityConfig.scheduledAsyncEntityActionTimeout() : 3;
		// set the receive timeout based into the configured timeout
		context().setReceiveTimeout(Duration.create(persistentEntityConfig.entityPassivateAfter(), TimeUnit.SECONDS));
	}

	/**
	 * to be implemented by the concrete implementation of the persistent entity for thr actual aggregate logic
	 *
	 * @param state the current state
	 * @return the init behavior to use for the persistent entity
	 */
	protected abstract ExecutionFlow<C, E, S> executionFlow(S state);

	/**
	 * Returns the initial value the state should be, when the actor is just created.
	 * <p>
	 * Although the implementation of this method is free to investigate the actor's context() and its environment, it must
	 * not apply any changes (i.e. be without side-effect).
	 */
	protected abstract S initialState();

	/**
	 * The flow :
	 * 1- receive command that implement reply type
	 * 2- receive ReceiveTimeout which will trigger passivate
	 * 3- receive stop msg based into passivation to gracefully stop the entity
	 * 4- unknown message so it will go to unhandled dead mail box
	 * <p>
	 * when it receive a valid command , it will trigger the following flow
	 * 1- generate Persist none -> the command is read only
	 * 2- generate persis one  -> the command will generate one event and after applying event via event handler , it will check if there after all persist callback to do
	 * 3- generate persist all  -> the command will generate list of events and after applying events via event handler , it will check if there after all persist callback to do
	 * <p>
	 * there is snapshot logic if the number of events reach the configured one to trigger snapshot also here
	 *
	 * @return Recieve logic for command handling
	 */
	@Override
	public final Receive createReceive() {
		return ReceiveBuilder.create()
				.match(commandType, command -> {
					if (executionFlow(getState()).getAsyncOnCommand().get(command.getClass()) != null) {
						FlowContext ctx = createCommandContext();
						// stash all commands till we receive the async result and use pipe to to send back
						// the response to the persistent actor
						context().become(waitingForAsyncAction, false);
						// schedule guard action to resume processing if the async action is taking more than timeout S seconds
						// to not starve the actor
						context().system().scheduler().scheduleOnce(Duration.create(asyncTimeout, TimeUnit.SECONDS),
								self(),
								new ResumeProcessing(),
								context().system().dispatchers().lookup(asyncDispatcherName),
								self());
						pipe(executionFlow(getState()).getAsyncOnCommand().get(command.getClass()).apply(command, ctx, getState())
								.exceptionally(throwable ->
										AsyncResult.<E>builder()
												.errorResponse(ErrorResponse.builder()
														.exceptionMsg(throwable.getLocalizedMessage())
														.build())
												.build()), context().system().dispatchers().lookup(asyncDispatcherName))
								.pipeTo(self(), sender());
					} else if (executionFlow(getState()).getOnCommand().get(command.getClass()) != null) {
						FlowContext ctx = createCommandContext();
						final Persist<E> nextAction = executionFlow(getState()).getOnCommand().get(command.getClass()).apply(command, ctx, getState());
						handlePostCommandPersistAction(nextAction);

					} else {
						getSender().tell(String.join("Unhandled command" + command.getClass().getSimpleName() + "in: " + this.getClass().getSimpleName() + "with id: " + persistenceIdPrefix), self());
						unhandled(command);
					}
				})
				.matchEquals(ReceiveTimeout.getInstance(), msg -> passivate())
				.match(Stop.class, msg -> context().stop(self()))
				.match(Object.class, this::unhandled)
				// in case of events replication to other data centers
				.match(eventType, this::applyEvent)
				.build();

	}

	/**
	 * @param nextAction the persist next action
	 *                   the main logic to see what is the action to be done after applying the command handler
	 *                   to see what events needed to be persisted and if there is post action needed to be done after persisting
	 *                   1- generate Persist none -> the command is read only
	 *                   2- generate persis one  -> the command will generate one event and after applying event via event handler , it will check if there after all persist callback to do
	 *                   3- generate persist all  -> the command will generate list of events and after applying events via event handler , it will check if there after all persist callback to do
	 */
	private void handlePostCommandPersistAction(Persist<E> nextAction) {

		if (nextAction instanceof PersistNone) {
			// no events to be persisted so it is read only action
		} else if (nextAction instanceof PersistOne) {
			final E event = nextAction.getEvent();
			if (event != null) {
				applyEvent(event);
				persist(tagged(event), evt -> {
					try {
						eventCount += 1;
						if (nextAction.getAfterPersist() != null)
							nextAction.getAfterPersist().accept((E) evt.payload());
						if (snapshotAfter > 0 && eventCount % snapshotAfter == 0)
							saveSnapshot(getState());
					} catch (Exception e) {
						log.error("error has been thrown during persist of the events", e);
						self().tell(ErrorResponse.builder().exceptionMsg(e.getLocalizedMessage()).errorCode("SA_001")
								.errorMsg("Error in connecting to event store").build(), getSender());
					}
				});
			} else {
				// just apply the post action with null event
				log.warning("NULL event is returned from persistOne action for entity id {}", persistenceId());
				if (nextAction.getAfterPersist() != null) {
					nextAction.getAfterPersist().accept(event);
				}

			}
		} else if (nextAction instanceof PersistAll) {

			final PersistAll<E> persistAllAction = (PersistAll) nextAction;

			if (persistAllAction.getEvents().isEmpty()) {
				log.warning("NO events is returned from persistOne action for entity id {}", persistenceId());
				if (persistAllAction.getAfterPersistAll() != null) {
					persistAllAction.getAfterPersistAll().accept(Collections.EMPTY_LIST);
				}
			} else {
				try {
					AtomicInteger count = new AtomicInteger(persistAllAction.getEvents().size());
					AtomicBoolean snap = new AtomicBoolean(false);
					persistAllAction.getEvents().forEach(this::applyEvent);
					persistAll(tagged(persistAllAction.getEvents()), evt -> {
						eventCount += 1;
						final int currentCount = count.decrementAndGet();
						if (currentCount == 0 && persistAllAction.getAfterPersist() != null) {
							persistAllAction.getAfterPersistAll().accept(persistAllAction.getEvents());
						}
						if (snapshotAfter > 0 && eventCount % snapshotAfter == 0) {
							snap.getAndSet(true);
						}
						if (currentCount == 0 && snap.get())
							saveSnapshot(getState());

					});
				} catch (Exception e) {
					log.error("error has been thrown during persist of the events", e);
					self().tell(ErrorResponse.builder().exceptionMsg(e.getLocalizedMessage()).errorCode("SA_001")
							.errorMsg("Error in connecting to event store").build(), getSender());
				}

			}

		}

	}

	/**
	 * Handle the following cases :
	 * 1- if there is a snapshot , apply the snapshot state and stop
	 * 2- else if it is an event reply without snapshot apply the event change and construct the state and behavior based into it
	 * 3 - if none then construct empty state
	 * <p>
	 *
	 * @return Receive logic for recovery
	 */
	@Override
	public final Receive createReceiveRecover() {
		return ReceiveBuilder.create()
				.match(SnapshotOffer.class, snapshot ->
						this.state = (S) snapshot.snapshot())
				.match(RecoveryCompleted.class, recoveryCompleted ->
						this.state = recoveryCompleted(state)
				).match(eventType, event -> {
					applyEvent(event);
					eventCount += 1;
				})
				.build();

	}

	@Override
	public String persistenceId() {
		try {
			return String.join(persistenceIdPrefix, URLDecoder.decode(self().path().name(), StandardCharsets.UTF_8.name()));
		} catch (UnsupportedEncodingException e) {
			log.error("encoding issue with the persistence id ");
			return String.join(persistenceIdPrefix, self().path().name());
		}
	}

	/**
	 * @param cause exception
	 * @param event the event to be persisted
	 * @param seqNr the currents sequence number
	 */
	@Override
	public void onPersistRejected(Throwable cause, Object event, long seqNr) {
		sender().tell("Persist of " + event.getClass().getSimpleName() + " rejected in" + persistenceId() +
				"caused by: " + cause.getLocalizedMessage(), ActorRef.noSender());
		super.onPersistRejected(cause, event, seqNr);

	}

	/**
	 * @param cause exception
	 * @param event the event to be persisted
	 * @param seqNr the currents sequence number
	 */
	@Override
	public void onPersistFailure(Throwable cause, Object event, long seqNr) {
		sender().tell("Persist of " + event.getClass().getSimpleName() + " failed in" + persistenceId() +
				"caused by: " + cause.getLocalizedMessage(), ActorRef.noSender());
		super.onPersistFailure(cause, event, seqNr);
	}

	/**
	 * the method that apply event to the current state and change behavior for the next updated behavior
	 *
	 * @param event the event to be applied to the current actor state
	 */
	private void applyEvent(final E event) {
		if (executionFlow(getState()).getOnEvent().get(event.getClass()) != null) {
			// get the new behavior by applying the behavior changing event handler
			this.state = executionFlow(getState()).getOnEvent().get(event.getClass()).apply(event, getState());

		} else {
			log.warning("no handler for event {} for entity id", event.getClass(), persistenceIdPrefix);
		}
	}

	/**
	 * @return new command context for each new received command
	 */
	private FlowContext createCommandContext() {
		return new FlowContext() {
			private final ActorRef replyTo = sender();

			@Override
			public <T> void reply(T msg) {
				replyTo.tell(msg, self());
			}
		};
	}

	/**
	 * Signals the parent actor (which is expected to be a ShardRegion) to passivate this actor, as a result
	 * of not having received any messages for a certain amount of time.
	 * <p>
	 * You can also invoke this method directly if you want to cleanly stop the actor explicitly.
	 */
	protected void passivate() {
		context().parent().tell(new ShardRegion.Passivate(STOP), self());
	}

	/**
	 * @param state the new state
	 * @return a new immutable behavior builder based into the new state
	 */
	protected final ExecutionFlow.ExecutionFlowBuilder<C, E, S> newFlowBuilder(S state) {
		return ExecutionFlow.<C, E, S>builder().state(state);
	}

	/**
	 * @return the new behavior after the recovery is done , it will be called internally by the internal recover api
	 */
	private S recoveryCompleted(S state) {
		this.state = state;
		return this.state;
	}

	/**
	 * Wraps the given event in a Tagged object, instructing the journal to add a tag to it.
	 */
	private Tagged tagged(E event) {
		Set<String> set = new HashSet<>();
		set.add(getEventTag(event));
		return new Tagged(event, set);
	}

	/**
	 * Wraps the given event in a Tagged object, instructing the journal to add a tag to it.
	 */
	private Iterable<Tagged> tagged(Iterable<E> event) {
		return Vector.ofAll(event).map(this::tagged);
	}

	/**
	 * @param eventType the event
	 * @return the configured or default tag for the event type
	 */
	private String getEventTag(E eventType) {
		return tags.getOrDefault(eventType.getClass(), ConfigValueFactory.fromAnyRef(eventType.getClass().getSimpleName()).render());
	}

	/**
	 * @return current state object if any
	 */
	protected S getState() {
		return state;
	}

	private static final class Stop implements Serializable {
		private static final long serialVersionUID = 1L;
	}

}
