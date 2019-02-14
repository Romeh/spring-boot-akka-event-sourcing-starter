package com.spring.akka.eventsourcing;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import com.spring.akka.eventsourcing.example.OrderEntity;
import com.spring.akka.eventsourcing.example.OrderEntityProperties;
import com.spring.akka.eventsourcing.example.OrderState;
import com.spring.akka.eventsourcing.example.OrderStatus;
import com.spring.akka.eventsourcing.example.commands.OrderCmd;
import com.spring.akka.eventsourcing.example.response.Response;
import com.spring.akka.eventsourcing.persistance.eventsourcing.PersistentEntityBroker;

import akka.Done;
import akka.actor.ActorRef;
import akka.pattern.PatternsCS;
import akka.util.Timeout;

/**
 * These test cases are time sensitive and may (extremely rarely) fail if run with test suite because of GC pause etc.
 * Run them as part of separate suite or increase the expiry delay to higher number and adjust test cases delays accordingly.
 */
@SpringBootTest(classes = TestConfig.class)
@RunWith(SpringRunner.class)
public class EntityActorTest {

	@Autowired
	PersistentEntityBroker persistentEntityBroker;

	@Autowired
	OrderEntityProperties actorProperties;

	@Test
	public void actorShouldGoToDifferentStagesProperly() {

		ActorRef testActorEntity = persistentEntityBroker.findPersistentEntity(OrderEntity.class);

		// check the response with order status is created
		final CompletableFuture<Object> result = PatternsCS.ask(testActorEntity, new OrderCmd.CreateCmd("123456"), Timeout.apply(
				5, TimeUnit.SECONDS)).toCompletableFuture();


		result.whenComplete((o, throwable) -> {
			Response response = (Response) o;
			System.out.println("step 1: " + response.toString());
			Assert.assertEquals("123456", response.getOrderId());
			Assert.assertEquals(response.getOrderStatus(), OrderStatus.Created.name());

		});

		pauseSeconds(5);

		if (result.isDone()) {
			final CompletableFuture<Object> result2 = PatternsCS.ask(testActorEntity, new OrderCmd.ValidateCmd("123456"), 5000).toCompletableFuture();
			result2.whenComplete((o, throwable) -> {
				Response response = (Response) o;
				System.out.println("step 2: " + response.toString());
				Assert.assertEquals("123456", response.getOrderId());
				Assert.assertEquals(response.getOrderStatus(), OrderStatus.Validated.name());

			});

			pauseSeconds(5);

			if (result2.isDone()) {
				final CompletableFuture<Object> result3 = PatternsCS.ask(testActorEntity, new OrderCmd.SignCmd("123456"), 5000).toCompletableFuture();
				result3.whenComplete((o, throwable) -> {
					Response response = (Response) o;
					System.out.println("step 3: " + response.toString());
					Assert.assertEquals("123456", response.getOrderId());
					Assert.assertEquals(response.getOrderStatus(), OrderStatus.Signed.name());
				});

				pauseSeconds(5);

				if (result3.isDone()) {
					final CompletableFuture<Object> result4 = PatternsCS.ask(testActorEntity, new OrderCmd.GetOrderStatusCmd("123456"), 5000).toCompletableFuture();
					result4.whenComplete((o, throwable) -> {
						OrderState response = (OrderState) o;
						System.out.println("step 4: " + response.toString());
						Assert.assertEquals(3, response.getEventsHistory().size());
						Assert.assertEquals(OrderStatus.Signed.name(), response.getOrderStatus());
					});

					pauseSeconds(5);

					if (result4.isDone()) {

						PatternsCS.ask(testActorEntity, new OrderCmd.SignCmd("123456"), 5000).whenComplete((o, throwable) -> {

							Assert.assertTrue(o instanceof Done);
							System.out.println("step 5: " + o.toString());
						});
					}


				}
			}
		}

	}

	private void pauseSeconds(int seconds) {
		try {
			Thread.sleep(seconds * 1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
