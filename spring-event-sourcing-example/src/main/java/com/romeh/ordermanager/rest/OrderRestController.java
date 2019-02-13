package com.romeh.ordermanager.rest;

import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.romeh.ordermanager.entities.OrderState;
import com.romeh.ordermanager.entities.Response;
import com.romeh.ordermanager.entities.commands.OrderCmd;
import com.romeh.ordermanager.reader.entities.JournalReadItem;
import com.romeh.ordermanager.reader.services.OrderNotFoundException;
import com.romeh.ordermanager.rest.dto.OrderRequest;
import com.romeh.ordermanager.services.OrdersBroker;

import io.swagger.annotations.Api;


/**
 * The main order domain REST API
 *
 * @author romeh
 */

@RestController
@RequestMapping("/orders")
@Api(value = "Order Manager REST API demo")
public class OrderRestController {

	@Autowired
	private OrdersBroker ordersBroker;

	/**
	 * @param orderRequest json order request
	 * @return ASYNC generic JSON response
	 */
	@RequestMapping(method = RequestMethod.POST)
	public CompletableFuture<Response> createOrder(@RequestBody @Valid OrderRequest orderRequest) {
		return ordersBroker.createOrder(new OrderCmd.CreateCmd(UUID.randomUUID().toString(), orderRequest.getOrderDetails()));

	}

	/**
	 * @param validateCmd validate order command JSON
	 * @return ASYNC generic JSON response
	 */
	@RequestMapping(value = "validate", method = RequestMethod.POST)
	public CompletableFuture<Response> validateOrder(@RequestBody @Valid OrderCmd.ValidateCmd validateCmd) {
		return ordersBroker.validateOrder(validateCmd);

	}

	/**
	 * @param signCmd sign order command JSON
	 * @return ASYNC generic JSON response
	 */
	@RequestMapping(value = "sign", method = RequestMethod.POST)
	public CompletableFuture<Response> signOrder(@RequestBody @Valid OrderCmd.SignCmd signCmd) {
		return ordersBroker.signeOrder(signCmd);

	}

	/**
	 * @param orderId unique orderId string value
	 * @return ASYNC OrderState Json response
	 */
	@RequestMapping(value = "/{orderId}", method = RequestMethod.GET)
	public CompletableFuture<OrderState> getOrderState(@PathVariable @NotNull String orderId) {
		return ordersBroker.getOrderStatus(new OrderCmd.GetOrderStatusCmd(orderId, Collections.emptyMap()));

	}


	/**
	 * @param orderId unique orderId string value
	 * @return JournalReadItem Json response if any
	 */
	@RequestMapping(value = "/readStore/{orderId}", method = RequestMethod.GET)
	public JournalReadItem getOrderLastStateFromReadStore(@PathVariable @NotNull String orderId) throws OrderNotFoundException {
		return ordersBroker.getOrderLastStatus(orderId);
	}

}
