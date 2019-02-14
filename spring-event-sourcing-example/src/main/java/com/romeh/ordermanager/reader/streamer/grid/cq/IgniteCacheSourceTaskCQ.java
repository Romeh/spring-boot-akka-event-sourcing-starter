/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.romeh.ordermanager.reader.streamer.grid.cq;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import javax.cache.Cache;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.event.EventType;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheEntryEventSerializableFilter;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.lang.IgniteAsyncCallback;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.romeh.ordermanager.reader.streamer.IgniteSource;
import com.romeh.ordermanager.reader.streamer.IgniteSourceConstants;

/**
 * Task to consume remote cluster cache events from the grid and inject them into Kafka.
 * <p>
 * Note that a task will create a bounded queue in the grid for more reliable data transfer.
 * Queue size can be changed by {@link IgniteSourceConstants#INTL_BUF_SIZE}.
 */
public class IgniteCacheSourceTaskCQ implements IgniteSource<CacheEntryEvent> {
	/**
	 * Logger.
	 */
	private static final Logger log = LoggerFactory.getLogger(IgniteCacheSourceTaskCQ.class);

	/**
	 * Event buffer size.
	 */
	private int evtBufSize = 1000;
	/**
	 * Event buffer.
	 */
	private BlockingQueue<CacheEntryEvent> evtBuf = new LinkedBlockingQueue<>(evtBufSize);
	/**
	 * Flag for stopped state.
	 */
	private final AtomicBoolean stopped = new AtomicBoolean(true);
	/**
	 * Max number of events taken from the buffer at once.
	 */
	private int evtBatchSize = 10;
	/**
	 * Local listener.
	 */
	private final TaskLocalListener locLsnr;


	private final QueryCursor<Cache.Entry<Object, BinaryObject>> cursor;


	public IgniteCacheSourceTaskCQ(Map<String, String> props, Ignite igniteNode) {
		Objects.requireNonNull(igniteNode);
		/**
		 * Cache name.
		 */
		String cacheName = props.get(IgniteSourceConstants.CACHE_NAME);

		if (props.containsKey(IgniteSourceConstants.INTL_BUF_SIZE)) {
			evtBufSize = Integer.parseInt(props.get(IgniteSourceConstants.INTL_BUF_SIZE));
			evtBuf = new LinkedBlockingQueue<>(evtBufSize);
		}

		if (props.containsKey(IgniteSourceConstants.INTL_BATCH_SIZE))
			evtBatchSize = Integer.parseInt(props.get(IgniteSourceConstants.INTL_BATCH_SIZE));

		try {
			locLsnr = new TaskLocalListener(this::handleCacheEvent);
			CacheEntryFilter rmtLsnr = new CacheEntryFilter(startSourceCacheListeners(props));
			// Creating a continuous query.
			ContinuousQuery<Object, BinaryObject> qry = new ContinuousQuery<>();
			qry.setLocalListener(locLsnr::apply);
			qry.setRemoteFilterFactory(() -> rmtLsnr);
			cursor = igniteNode.cache(cacheName).withKeepBinary().query(qry);

		} catch (Exception e) {
			log.error("Failed to register event listener!", e);
			throw new IllegalStateException(e);
		} finally {
			stopped.set(false);
		}
	}

	@SuppressWarnings("unchecked")
	private static IgnitePredicate<CacheEntryEvent> startSourceCacheListeners(Map<String, String> props) {

		IgnitePredicate<CacheEntryEvent> ignitePredicate = null;
		if (props.containsKey(IgniteSourceConstants.CACHE_FILTER_CLASS)) {
			String filterCls = props.get(IgniteSourceConstants.CACHE_FILTER_CLASS);
			if (filterCls != null && !filterCls.isEmpty()) {
				try {
					Class<? extends IgnitePredicate<CacheEntryEvent>> clazz =
							(Class<? extends IgnitePredicate<CacheEntryEvent>>) Class.forName(filterCls);
					ignitePredicate = clazz.newInstance();
				} catch (Exception e) {
					log.error("Failed to instantiate the provided filter! " +
							"User-enabled filtering is ignored!", e);
				}
			}
		}
		return ignitePredicate;
	}

	public boolean isStopped() {
		return stopped.get();
	}

	@Override
	public List<CacheEntryEvent> poll() {
		if (log.isDebugEnabled()) {
			log.debug("Cache source polling has been started !");
		}
		ArrayList<CacheEntryEvent> evts = new ArrayList<>(evtBatchSize);

		if (stopped.get())
			return evts;

		try {
			if (evtBuf.drainTo(evts, evtBatchSize) > 0) {
				if (log.isDebugEnabled()) {
					log.debug("Polled events {}", evts);
				}
				return evts;
			}
		} catch (IgniteException e) {
			log.error("Error when polling event queue!", e);
		}

		// for shutdown.
		return Collections.emptyList();
	}


	/**
	 * Stops the grid client.
	 */
	@Override
	public void stop() {
		if (stopped.get())
			return;

		if (stopped.compareAndSet(false, true)) {
			stopRemoteListen();
		}

	}

	/**
	 * Stops the remote listener.
	 */
	private void stopRemoteListen() {
		if (cursor != null)
			cursor.close();
	}

	/**
	 * Local listener buffering cache events to be further sent to Kafka.
	 */
	private static class TaskLocalListener implements IgnitePredicate<Iterable<CacheEntryEvent<?, ? extends BinaryObject>>> {
		/**
		 * {@inheritDoc}
		 */

		private final transient Consumer<CacheEntryEvent<?, ? extends BinaryObject>> evntBufferConsumer;

		public TaskLocalListener(Consumer<CacheEntryEvent<?, ? extends BinaryObject>> evntBufferConsumer) {
			this.evntBufferConsumer = evntBufferConsumer;
		}

		@Override
		public boolean apply(Iterable<CacheEntryEvent<?, ? extends BinaryObject>> cacheEntryEvents) {
			cacheEntryEvents.forEach(evt -> {
				if (evt.getEventType().equals(EventType.CREATED) || evt.getEventType().equals(EventType.UPDATED))
					evntBufferConsumer.accept(evt);
			});
			return true;
		}
	}


	public void handleCacheEvent(CacheEntryEvent<?, ? extends BinaryObject> evt) {
		try {
			if (!evtBuf.offer(evt, 10, TimeUnit.MILLISECONDS))
				log.error("Failed to buffer event {}", evt.getEventType());
		} catch (InterruptedException e) {
			log.error("error has been thrown in TaskLocalListener {} ", e);
			Thread.currentThread().interrupt();
		}
	}

	/**
	 * remote filer
	 */
	@IgniteAsyncCallback
	private static class CacheEntryFilter implements CacheEntryEventSerializableFilter<Object, BinaryObject> {
		/**
		 * Ignite instance.
		 */
		@IgniteInstanceResource
		private transient Ignite ignite;

		private final IgnitePredicate<CacheEntryEvent> filter;

		private CacheEntryFilter(IgnitePredicate<CacheEntryEvent> filter) {
			this.filter = filter;
		}


		@Override
		public boolean evaluate(CacheEntryEvent<?, ? extends BinaryObject> evt) throws CacheEntryListenerException {
			Affinity<Object> affinity = ignite.affinity(evt.getSource().getName());

			if (evt.getEventType().equals(EventType.CREATED) || evt.getEventType().equals(EventType.UPDATED) && affinity.isPrimary(ignite.cluster().localNode(), evt.getKey())) {
				// Process this event. Ignored on backups.
				return filter == null || !filter.apply(evt);
			}
			return false;
		}
	}

}
