package com.romeh.ordermanager.reader.services;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.configuration.CacheConfiguration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.stereotype.Component;

import com.romeh.ordermanager.reader.entities.JournalReadItem;
import com.romeh.ordermanager.reader.streamer.IgniteSinkConstants;
import com.romeh.ordermanager.reader.streamer.IgniteSourceConstants;
import com.romeh.ordermanager.reader.streamer.grid.streamer.IgniteCacheEventStreamerRx;

import akka.actor.AbstractActor;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import akka.persistence.ignite.common.entities.JournalStarted;
import akka.persistence.ignite.extension.IgniteExtension;
import akka.persistence.ignite.extension.IgniteExtensionProvider;

/**
 * the read side service to show the case how you can stream stored write event store to a read side store using Rx java and ignite streamer APIs
 *
 * @author romeh
 */
@Component
public class ReadStoreStreamerService implements ApplicationListener<ContextRefreshedEvent> {

	private static final String JOURNAL_CACHE = "akka-journal";
	private static final String READ_CACHE = "Read-Store";
	private final IgniteExtension igniteExtension;
	private final ActorSystem actorSystem;
	private final IgniteCache<String, JournalReadItem> readStore;

	@Autowired
	public ReadStoreStreamerService(ActorSystem actorSystem) {
		this.actorSystem = actorSystem;
		this.igniteExtension = IgniteExtensionProvider.EXTENSION.get(actorSystem);
		// make sure the read store cache is created
		//usually you should not need that as the writer and reader should be different nodes
		this.readStore = getOrCreateReadStoreCache();
	}


	/**
	 * @param orderId order id to get the status for
	 * @return the order status if any
	 */
	public JournalReadItem getOrderStatus(String orderId) throws OrderNotFoundException {
		return Optional.ofNullable(readStore.get(orderId))
				.orElseThrow(() -> new OrderNotFoundException("order is not found in the read store"));
	}

	/**
	 * @param contextStartedEvent the spring context started event
	 *                            start the events streamer once the spring context init is finished
	 */
	@Override
	public void onApplicationEvent(ContextRefreshedEvent contextStartedEvent) {
		actorSystem.actorOf(Props.create(IgniteStreamerStarter.class, IgniteStreamerStarter::new), "IgniteStreamerActor");

	}

	/**
	 * start the event streamer from the journal write event store to the read side cache store which store only query intersted data
	 */
	private void startIgniteStreamer() {
		// streamer parameters
		Map<String, String> sourceMap = new HashMap<>();
		sourceMap.put(IgniteSourceConstants.CACHE_NAME, JOURNAL_CACHE);
		Map<String, String> sinkMap = new HashMap<>();
		sinkMap.put(IgniteSinkConstants.CACHE_NAME, READ_CACHE);
		sinkMap.put(IgniteSinkConstants.CACHE_ALLOW_OVERWRITE, "true");
		sinkMap.put(IgniteSinkConstants.SINGLE_TUPLE_EXTRACTOR_CLASS, ReadStoreExtractor.class.getName());
		// start the streamer
		final IgniteCacheEventStreamerRx journalStreamer = IgniteCacheEventStreamerRx.builderWithContinuousQuery()
				.pollingInterval(500)
				.flushBufferSize(5)
				.retryTimesOnError(2)
				.sourceCache(sourceMap, igniteExtension.getIgnite())
				.sinkCache(sinkMap, igniteExtension.getIgnite())
				.build();

		journalStreamer.execute();
	}

	/**
	 * check if the read side cache is already created or create it if missing
	 * usually you should not need that as the writer and reader should be different nodes
	 */
	private IgniteCache<String, JournalReadItem> getOrCreateReadStoreCache() {
		IgniteCache<String, JournalReadItem> cache = igniteExtension.getIgnite().cache(READ_CACHE);

		if (null == cache) {
			CacheConfiguration<String, JournalReadItem> readItemCacheConfiguration = new CacheConfiguration<>();
			readItemCacheConfiguration.setBackups(1);
			readItemCacheConfiguration.setName(READ_CACHE);
			readItemCacheConfiguration.setAtomicityMode(CacheAtomicityMode.ATOMIC);
			readItemCacheConfiguration.setCacheMode(CacheMode.PARTITIONED);
			readItemCacheConfiguration.setQueryEntities(Collections.singletonList(createJournalBinaryQueryEntity()));
			cache = igniteExtension.getIgnite().getOrCreateCache(readItemCacheConfiguration);
		}
		return cache;
	}

	/**
	 * @return QueryEntity which the binary query definition of the binary object stored into the read side cache
	 */
	private QueryEntity createJournalBinaryQueryEntity() {
		QueryEntity queryEntity = new QueryEntity();
		queryEntity.setValueType(JournalReadItem.class.getName());
		queryEntity.setKeyType(String.class.getName());
		LinkedHashMap<String, String> fields = new LinkedHashMap<>();
		fields.put(JournalReadItem.ORDER_ID, String.class.getName());
		fields.put(JournalReadItem.STATUS_FIELD, String.class.getName());
		queryEntity.setFields(fields);
		queryEntity.setIndexes(Arrays.asList(new QueryIndex(JournalReadItem.ORDER_ID),
				new QueryIndex(JournalReadItem.STATUS_FIELD)));
		return queryEntity;

	}

	/**
	 * simple akka actor to listen to the journal started event so it can start the event store streamer once the persistence store is running
	 * usually you should not need that as the writer and reader should be different nodes but as here for the sake of the example we have reader and writer
	 * running in the same server ignite node
	 */
	private final class IgniteStreamerStarter extends AbstractActor {

		@Override
		public void preStart() {
			getContext().getSystem().eventStream().subscribe(getSelf(), JournalStarted.class);
		}

		@Override
		public Receive createReceive() {
			// start the streamer once it receive the journal started event
			return ReceiveBuilder.create().match(JournalStarted.class, journalStarted -> startIgniteStreamer()).build();
		}
	}
}
