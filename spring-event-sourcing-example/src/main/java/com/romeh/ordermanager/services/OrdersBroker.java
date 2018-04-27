package com.romeh.ordermanager.services;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.romeh.ordermanager.domain.OrderManager;
import com.romeh.ordermanager.entities.OrderState;
import com.romeh.ordermanager.entities.Response;
import com.romeh.ordermanager.entities.commands.OrderCmd;
import com.spring.akka.eventsourcing.persistance.eventsourcing.PersistentEntityBroker;

import akka.actor.ActorRef;
import akka.pattern.PatternsCS;
import akka.util.Timeout;

/**
 * the orders service that handle order commands and respond in async mode
 *
 * @author romeh
 */
@Service
public class OrdersBroker {

	/**
	 * the AKKA sharding persistent entities general broker
	 */
	private final PersistentEntityBroker persistentEntityBroker;
	private final static Timeout timeout=Timeout.apply(
			2, TimeUnit.SECONDS);
	/**
	 * generic completable future handle response function
	 */
	private final Function<Object, Response> handlerResponse = o -> {

		if (o != null && o instanceof Response) {
			return (Response) o;
		}
		else {
			return Response.builder().errorCode("1100").errorMessage("unexpected error has been found").build();
		}

	};

	/**
	 * generic completable future get order state handle response function
	 */
	private final Function<Object, OrderState> handleGetState = o -> {

		if (o != null) {
			return (OrderState) o;
		} else {
			throw new IllegalStateException("un-expected error has been thrown");
		}

	};
	/**
	 * generic completable future handle exception function
	 */
	private final Function<Throwable, Response> handleException = throwable -> Response.builder().errorCode("1111").errorMessage(throwable.getLocalizedMessage()).build();


	@Autowired
	public OrdersBroker(PersistentEntityBroker persistentEntityBroker) {
		this.persistentEntityBroker = persistentEntityBroker;
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
	 * get order state service API
	 *
	 * @param getOrderStatusCmd get Order state command
	 * @return order state
	 */
	public CompletableFuture<OrderState> getOrderStatus(OrderCmd.GetOrderStatusCmd getOrderStatusCmd) {
		return PatternsCS.ask(getOrderEntity(), getOrderStatusCmd, timeout).toCompletableFuture()
				.thenApply(handleGetState);
	}

	/**
	 * @return Persistent entity actor reference based into AKKA cluster sharding
	 */
	private final ActorRef getOrderEntity() {
		return persistentEntityBroker.findPersistentEntity(OrderManager.class);

	}


}
