package com.romeh.ordermanager.reader.services;

import java.io.NotSerializableException;
import java.util.Map;

import javax.cache.event.CacheEntryEvent;

import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.stream.StreamSingleTupleExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.romeh.ordermanager.entities.events.OrderEvent;
import com.romeh.ordermanager.reader.entities.JournalReadItem;
import com.romeh.ordermanager.serializer.OrderManagerSerializer;

import akka.persistence.ignite.common.enums.FieldNames;
import akka.persistence.serialization.MessageFormats;
import akka.protobuf.InvalidProtocolBufferException;

/**
 * @author romeh
 */
public class ReadStoreExtractor implements StreamSingleTupleExtractor<CacheEntryEvent<Object, BinaryObject>, String, JournalReadItem> {
	private static final Logger log = LoggerFactory.getLogger(ReadStoreExtractor.class);
	private static final OrderManagerSerializer orderOrderManagerSerializer = new OrderManagerSerializer();

	@Override
	public Map.Entry<String, JournalReadItem> extract(CacheEntryEvent<Object, BinaryObject> msg) {

		IgniteBiTuple<String, JournalReadItem> readRecord = null;
		final BinaryObject value = msg.getValue();
		final String orderId = value.field(FieldNames.persistenceId.name());
		final byte[] payload = value.field(FieldNames.payload.name());
		try {
			final MessageFormats.PersistentMessage persistentMessage = MessageFormats.PersistentMessage.parseFrom(payload);
			if (persistentMessage.hasPayload()) {
				final OrderEvent event = (OrderEvent) orderOrderManagerSerializer.fromBinary(persistentMessage.getPayload().getPayload().toByteArray(), persistentMessage.getPayload().getPayloadManifest().toStringUtf8());
				readRecord = new IgniteBiTuple<>(event.getOrderId(), new JournalReadItem(event.getOrderId(), event.getOrderStatus().name()));
			}
		} catch (InvalidProtocolBufferException | NotSerializableException e) {
			log.error("error in parsing the msg for id {} with error {}", orderId, e);
		}

		return readRecord;

	}
}
