package com.romeh.ordermanager.services;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.romeh.ordermanager.domain.OrderManager;
import com.romeh.ordermanager.entities.OrderState;
import com.romeh.ordermanager.entities.Response;
import com.romeh.ordermanager.entities.commands.OrderCmd;
import com.romeh.ordermanager.reader.entities.JournalReadItem;
import com.romeh.ordermanager.reader.services.OrderNotFoundException;
import com.romeh.ordermanager.reader.services.ReadStoreStreamerService;
import com.spring.akka.eventsourcing.persistance.eventsourcing.PersistentEntityBroker;

import akka.actor.ActorRef;
import akka.pattern.PatternsCS;

/**
 * the orders service that handle order commands and respond in async mode
 *
 * @author romeh
 */
@Service
public class OrdersBroker {

	private final static Duration timeout = Duration.ofMillis(2000);
	/**
	 * the AKKA sharding persistent entities general broker
	 */
	private final PersistentEntityBroker persistentEntityBroker;
	private final ReadStoreStreamerService readStoreStreamerService;
	/**
	 * generic completable future handle response function
	 */
	private final Function<Object, Response> handlerResponse = o -> {
		if (o instanceof Response) {
			return (Response) o;
		} else {
			return Response.builder().errorCode("1100").errorMessage("unexpected error has been found").build();
		}
	};

	/**
	 * generic completable future get order state handle response function
	 */
	private final Function<Object, OrderState> handleGetState = o -> Optional.ofNullable(o).map(getState -> (OrderState) getState)
			.orElseThrow(() -> new IllegalStateException("un-expected error has been thrown"));

	/**
	 * generic completable future handle exception function
	 */
	private final Function<Throwable, Response> handleException = throwable -> Response.builder().errorCode("1111").errorMessage(throwable.getLocalizedMessage()).build();


	@Autowired
	public OrdersBroker(PersistentEntityBroker persistentEntityBroker, ReadStoreStreamerService readStoreStreamerService) {
		this.persistentEntityBroker = persistentEntityBroker;
		this.readStoreStreamerService = readStoreStreamerService;
	}

	/**
	 * create order service API
	 *
	 * @param createCmd create order command
	 * @return generic response object
	 */
	public CompletableFuture<Response> createOrder(OrderCmd.CreateCmd createCmd) {

		return PatternsCS.ask(getOrderEntity(), createCmd, timeout).toCompletableFuture()
				.thenApply(handlerResponse).exceptionally(handleException);
	}

	/**
	 * validate order service API
	 *
	 * @param validateCmd validate order command
	 * @return generic response object
	 */
	public CompletableFuture<Response> validateOrder(OrderCmd.ValidateCmd validateCmd) {
		return PatternsCS.ask(getOrderEntity(), validateCmd, timeout).toCompletableFuture()
				.thenApply(handlerResponse).exceptionally(handleException);
	}
	/**
	 * Sign order service API
	 *
	 * @param signCmd sign order command
	 * @return generic response object
	 */
	public CompletableFuture<Response> signeOrder(OrderCmd.SignCmd signCmd) {
		return PatternsCS.ask(getOrderEntity(), signCmd, timeout).toCompletableFuture()
				.thenApply(handlerResponse).exceptionally(handleException);
	}

	/**
	 * get order state service API from write store
	 *
	 * @param getOrderStatusCmd get Order state command
	 * @return order state
	 */
	public CompletableFuture<OrderState> getOrderStatus(OrderCmd.GetOrderStatusCmd getOrderStatusCmd) {
		return PatternsCS.ask(getOrderEntity(), getOrderStatusCmd, timeout).toCompletableFuture()
				.thenApply(handleGetState);
	}

	/**
	 * get last order state service API from read store
	 *
	 * @param orderId the order id
	 * @return order last status
	 */
	public JournalReadItem getOrderLastStatus(String orderId) throws OrderNotFoundException {
		return readStoreStreamerService.getOrderStatus(orderId);
	}

	/**
	 * @return Persistent entity actor reference based into AKKA cluster sharding
	 */
	private ActorRef getOrderEntity() {
		return persistentEntityBroker.findPersistentEntity(OrderManager.class);

	}


}
