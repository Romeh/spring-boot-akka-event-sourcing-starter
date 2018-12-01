package com.romeh.ordermanager.serializer;


import java.io.NotSerializableException;

import com.google.protobuf.InvalidProtocolBufferException;
import com.romeh.ordermanager.entities.commands.OrderCmd;
import com.romeh.ordermanager.entities.enums.OrderStatus;
import com.romeh.ordermanager.entities.events.CreatedEvent;
import com.romeh.ordermanager.entities.events.FinishedEvent;
import com.romeh.ordermanager.entities.events.OrderEvent;
import com.romeh.ordermanager.entities.events.SignedEvent;
import com.romeh.ordermanager.entities.events.ValidatedEvent;
import com.romeh.ordermanager.protobuf.EventsAndCommands;

import akka.serialization.SerializerWithStringManifest;

/**
 * @author romeh
 */
public class OrderManagerSerializer extends SerializerWithStringManifest {


	private static final String CREATED_EVENT = "CreatedEvent";
	private static final String VALIDATED_EVENT = "ValidatedEvent";
	private static final String SIGNED_EVENT = "SignedEvent";
	private static final String FINISHED_EVENT = "FinishedEvent";
	private static final String CREATE_COMMAND = "CreateCommand";
	private static final String VALIDATE_COMMAND = "ValdiateCommand";
	private static final String SIGN_COMMAND = "SignCommand";
	private static final String GET_COMMAND = "GetCommand";
	private static final String ASYNC_COMMAND = "AsyncCommand";


	@Override
	public int identifier() {
		return 100;
	}

	@Override
	public String manifest(Object o) {
		if (o instanceof OrderCmd.CreateCmd)
			return CREATE_COMMAND;
		if (o instanceof OrderCmd.ValidateCmd)
			return VALIDATE_COMMAND;
		if (o instanceof OrderCmd.SignCmd)
			return SIGN_COMMAND;
		if (o instanceof OrderCmd.GetOrderStatusCmd)
			return GET_COMMAND;
		if (o instanceof OrderCmd.AsyncSignCmd)
			return ASYNC_COMMAND;
		if (o instanceof CreatedEvent)
			return CREATED_EVENT;
		if (o instanceof SignedEvent)
			return SIGNED_EVENT;
		if (o instanceof ValidatedEvent)
			return VALIDATED_EVENT;
		if (o instanceof FinishedEvent)
			return FINISHED_EVENT;
		else
			throw new IllegalArgumentException("Unknown type: " + o);
	}

	@Override
	public byte[] toBinary(Object o) {

		if (o instanceof OrderEvent) {
			OrderEvent orderEvent = (OrderEvent) o;
			return EventsAndCommands.OrderEvent.newBuilder()
					.setOrderId(orderEvent.getOrderId())
					.setOrderStatus(orderEvent.getOrderStatus().name())
					.build().toByteArray();

		} else if (o instanceof OrderCmd) {
			OrderCmd orderCmd = (OrderCmd) o;
			return EventsAndCommands.OrderCmd.newBuilder()
					.setOrderId(orderCmd.getOrderId())
					.putAllOrderDetails(orderCmd.getOrderDetails())
					.build().toByteArray();
		} else {
			throw new IllegalArgumentException("Cannot serialize object of type " + o.getClass().getName());
		}

	}

	@Override
	public Object fromBinary(byte[] bytes, String manifest) throws NotSerializableException {
		try {
			if (manifest.equals(CREATE_COMMAND)) {
				final EventsAndCommands.OrderCmd orderCmd = EventsAndCommands.OrderCmd.parseFrom(bytes);
				return new OrderCmd.CreateCmd(orderCmd.getOrderId(), orderCmd.getOrderDetailsMap());
			}
			if (manifest.equals(SIGN_COMMAND)) {
				final EventsAndCommands.OrderCmd orderCmd = EventsAndCommands.OrderCmd.parseFrom(bytes);
				return new OrderCmd.SignCmd(orderCmd.getOrderId(), orderCmd.getOrderDetailsMap());
			}
			if (manifest.equals(VALIDATE_COMMAND)) {
				final EventsAndCommands.OrderCmd orderCmd = EventsAndCommands.OrderCmd.parseFrom(bytes);
				return new OrderCmd.ValidateCmd(orderCmd.getOrderId(), orderCmd.getOrderDetailsMap());
			}
			if (manifest.equals(GET_COMMAND)) {
				final EventsAndCommands.OrderCmd orderCmd = EventsAndCommands.OrderCmd.parseFrom(bytes);
				return new OrderCmd.GetOrderStatusCmd(orderCmd.getOrderId(), orderCmd.getOrderDetailsMap());
			}
			if (manifest.equals(ASYNC_COMMAND)) {
				final EventsAndCommands.OrderCmd orderCmd = EventsAndCommands.OrderCmd.parseFrom(bytes);
				return new OrderCmd.AsyncSignCmd(orderCmd.getOrderId(), orderCmd.getOrderDetailsMap());
			}
			if (manifest.equals(CREATED_EVENT)) {
				final EventsAndCommands.OrderEvent orderEvent = EventsAndCommands.OrderEvent.parseFrom(bytes);
				return new CreatedEvent(orderEvent.getOrderId(), OrderStatus.valueOf(orderEvent.getOrderStatus()));
			}
			if (manifest.equals(SIGNED_EVENT)) {
				final EventsAndCommands.OrderEvent orderEvent = EventsAndCommands.OrderEvent.parseFrom(bytes);
				return new SignedEvent(orderEvent.getOrderId(), OrderStatus.valueOf(orderEvent.getOrderStatus()));
			}
			if (manifest.equals(VALIDATED_EVENT)) {
				final EventsAndCommands.OrderEvent orderEvent = EventsAndCommands.OrderEvent.parseFrom(bytes);
				return new ValidatedEvent(orderEvent.getOrderId(), OrderStatus.valueOf(orderEvent.getOrderStatus()));
			}
			if (manifest.equals(FINISHED_EVENT)) {
				final EventsAndCommands.OrderEvent orderEvent = EventsAndCommands.OrderEvent.parseFrom(bytes);
				return new FinishedEvent(orderEvent.getOrderId(), OrderStatus.valueOf(orderEvent.getOrderStatus()));
			} else {
				throw new NotSerializableException(
						"Unimplemented deserialization of message with manifest [" + manifest + "] in " + getClass().getName());
			}
		} catch (InvalidProtocolBufferException e) {
			throw new NotSerializableException(e.getMessage());
		}

	}
}
