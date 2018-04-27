package com.romeh.ordermanager.serializer;

import java.io.NotSerializableException;

import com.google.protobuf.InvalidProtocolBufferException;
import com.romeh.ordermanager.entities.commands.OrderCmd;
import com.romeh.ordermanager.entities.events.OrderEvent;
import com.romeh.ordermanager.protobuf.EventsAndCommands;

import akka.serialization.SerializerWithStringManifest;

/**
 * @author romeh
 */
public class OrderManagerSerializer extends SerializerWithStringManifest {


	private static final String ORDER_EVENT = "orderEvent";
	private static final String ORDER_COMMAND = "orderCommand";


	@Override
	public int identifier() {
		return 100;
	}

	@Override
	public String manifest(Object o) {
		if (o instanceof OrderCmd)
			return ORDER_COMMAND;
		else if (o instanceof OrderEvent)
			return ORDER_EVENT;
		else
			throw new IllegalArgumentException("Unknown type: " + o);
	}

	@Override
	public byte[] toBinary(Object o) {

		if(o instanceof OrderEvent){
			OrderEvent orderEvent=(OrderEvent)o;
			return EventsAndCommands.OrderEvent.newBuilder()
					.setOrderId(orderEvent.getOrderId())
					.setOrderStatus(orderEvent.getOrderStatus().name())
					.build().toByteArray();

		}else if(o instanceof OrderCmd){
			OrderCmd orderCmd=(OrderCmd) o;
			return EventsAndCommands.OrderCmd.newBuilder()
					.setOrderId(orderCmd.getOrderId())
					.putAllOrderDetails(orderCmd.getOrderDetails())
					.build().toByteArray();
		}else{
			throw new IllegalArgumentException("Cannot serialize object of type " + o.getClass().getName());
		}

	}

	@Override
	public Object fromBinary(byte[] bytes, String manifest) throws NotSerializableException {
		try {
			if (manifest.equals(ORDER_COMMAND)) {

				return EventsAndCommands.OrderCmd.parseFrom(bytes);

			} else if (manifest.equals(ORDER_EVENT)) {
				return EventsAndCommands.OrderEvent.parseFrom(bytes);
			} else {
				throw new NotSerializableException(
						"Unimplemented deserialization of message with manifest [" + manifest + "] in " + getClass().getName());
			}
		}catch (InvalidProtocolBufferException e) {
			throw new NotSerializableException(e.getMessage());
		}

	}
}
