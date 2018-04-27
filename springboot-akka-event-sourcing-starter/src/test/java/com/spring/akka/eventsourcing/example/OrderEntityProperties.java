package com.spring.akka.eventsourcing.example;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import javax.annotation.PostConstruct;

import org.springframework.stereotype.Component;

import com.spring.akka.eventsourcing.config.PersistentEntityProperties;
import com.spring.akka.eventsourcing.example.commands.OrderCmd;
import com.spring.akka.eventsourcing.example.events.CreatedEvent;
import com.spring.akka.eventsourcing.example.events.FinishedEvent;
import com.spring.akka.eventsourcing.example.events.OrderEvent;
import com.spring.akka.eventsourcing.example.events.SignedEvent;
import com.spring.akka.eventsourcing.example.events.ValidatedEvent;

@Component
public class OrderEntityProperties implements PersistentEntityProperties<OrderEntity, OrderCmd, OrderEvent> {

	private Map<Class<? extends OrderEvent>, String> tags;

	@PostConstruct
	public void init() {
		Map<Class<? extends OrderEvent>, String> init = new HashMap<>();
		init.put(CreatedEvent.class, "CreatedOrders");
		init.put(FinishedEvent.class, "FinishedOrders");
		init.put(SignedEvent.class, "SignedOrders");
		init.put(ValidatedEvent.class, "ValidatedOrders");
		tags = Collections.unmodifiableMap(init);

	}

	@Override
	public int snapshotStateAfter() {
		return 5;
	}

	@Override
	public long entityPassivateAfter() {
		return 60;
	}

	@Override
	public Map<Class<? extends OrderEvent>, String> tags() {

		return tags;
	}

	@Override
	public int numberOfShards() {
		return 20;
	}

	@Override
	public Function<OrderCmd, String> persistenceIdPostfix() {
		return OrderCmd::getOrderId;
	}

	@Override
	public String persistenceIdPrefix() {
		return OrderEntity.class.getSimpleName();
	}

	@Override
	public Class<OrderEntity> getEntityClass() {
		return OrderEntity.class;
	}

	@Override
	public Class<OrderCmd> getRootCommandType() {
		return OrderCmd.class;
	}

	@Override
	public Class<OrderEvent> getRootEventType() {
		return OrderEvent.class;
	}

	@Override
	public String asyncPersistentEntityDispatcherName() {
		return null;
	}

	@Override
	public String pipeDispatcherName() {
		return null;
	}

	@Override
	public long scheduledAsyncEntityActionTimeout() {
		return 3;
	}
}
