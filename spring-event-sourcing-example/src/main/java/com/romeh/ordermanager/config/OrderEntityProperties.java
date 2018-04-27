package com.romeh.ordermanager.config;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import javax.annotation.PostConstruct;

import org.springframework.stereotype.Component;

import com.romeh.ordermanager.domain.OrderManager;
import com.romeh.ordermanager.entities.commands.OrderCmd;
import com.romeh.ordermanager.entities.events.CreatedEvent;
import com.romeh.ordermanager.entities.events.FinishedEvent;
import com.romeh.ordermanager.entities.events.OrderEvent;
import com.romeh.ordermanager.entities.events.SignedEvent;
import com.romeh.ordermanager.entities.events.ValidatedEvent;
import com.spring.akka.eventsourcing.config.PersistentEntityProperties;

/**
 * the main order entity required configuration for event souring toolkit
 */
@Component
public class OrderEntityProperties implements PersistentEntityProperties<OrderManager, OrderCmd, OrderEvent> {

	private Map<Class<? extends OrderEvent>, String> tags;

	/**
	 * init the event tags map
	 */
	@PostConstruct
	public void init() {
		Map<Class<? extends OrderEvent>, String> init = new HashMap<>();
		init.put(CreatedEvent.class, "CreatedOrders");
		init.put(FinishedEvent.class, "FinishedOrders");
		init.put(SignedEvent.class, "SignedOrders");
		init.put(ValidatedEvent.class, "ValidatedOrders");
		tags = Collections.unmodifiableMap(init);

	}

	/**
	 * @return the entity should save snapshot of its state after how many persisted events
	 */
	@Override
	public int snapshotStateAfter() {
		return 5;
	}

	/**
	 * @return the entity should passivate after how long of being non actively serving requests
	 */
	@Override
	public long entityPassivateAfter() {
		return 60;
	}

	/**
	 * @return map of event class to tag value , used into event tagging before persisting the events into the event store by akka persistence
	 */
	@Override
	public Map<Class<? extends OrderEvent>, String> tags() {

		return tags;
	}

	/**
	 * @return number of cluster sharding for the entity
	 */
	@Override
	public int numberOfShards() {
		return 20;
	}

	/**
	 * @return persistenceIdPostfix function , used in cluster sharding entity routing
	 */
	@Override
	public Function<OrderCmd, String> persistenceIdPostfix() {
		return OrderCmd::getOrderId;
	}

	/**
	 * @return entity persistenceIdPrefix , used in cluster sharding routing
	 */
	@Override
	public String persistenceIdPrefix() {
		return OrderManager.class.getSimpleName();
	}

	/**
	 * @return entity class
	 */
	@Override
	public Class<OrderManager> getEntityClass() {
		return OrderManager.class;
	}

	/**
	 * @return main super root command type
	 */
	@Override
	public Class<OrderCmd> getRootCommandType() {
		return OrderCmd.class;
	}

	/**
	 * @return main super root event type
	 */
	@Override
	public Class<OrderEvent> getRootEventType() {
		return OrderEvent.class;
	}


	@Override
	public String asyncPersistentEntityDispatcherName() {
		return null;
	}

	/**
	 * @return the custom configurable dispatcher name used for async blocking IO operations in the persistent entity,
	 * to be used instead of the default akka actors dispatchers to not starve the actors thread dispatcher with blocking IO
	 */
	@Override
	public String pipeDispatcherName() {
		return null;
	}

	/**
	 * @return the timeout for any async command handler actions , used to monitor pipeTo actions
	 */
	@Override
	public long scheduledAsyncEntityActionTimeout() {
		return 3;
	}
}
