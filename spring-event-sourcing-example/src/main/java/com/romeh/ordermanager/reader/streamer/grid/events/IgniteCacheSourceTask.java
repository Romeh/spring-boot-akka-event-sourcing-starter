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

package com.romeh.ordermanager.reader.streamer.grid.events;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.events.CacheEvent;
import org.apache.ignite.events.EventType;
import org.apache.ignite.lang.IgniteBiPredicate;
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
public class IgniteCacheSourceTask implements IgniteSource<CacheEvent> {
	/**
	 * Logger.
	 */
	private static final Logger log = LoggerFactory.getLogger(IgniteCacheSourceTask.class);

	/**
	 * Event buffer size.
	 */
	private static int evtBufSize = 1000;
	/**
	 * Cache name.
	 */
	private static String cacheName;
	/**
	 * Local listener.
	 */
	private static final TaskLocalListener locLsnr = new TaskLocalListener();
	/**
	 * User-defined filter.
	 */
	private static IgnitePredicate<CacheEvent> filter;
	/**
	 * Event buffer.
	 */
	private static BlockingQueue<CacheEvent> evtBuf = new LinkedBlockingQueue<>(evtBufSize);
	/**
	 * Flag for stopped state.
	 */
	private final transient AtomicBoolean stopped = new AtomicBoolean(true);
	private final transient Ignite sourceNode;
	/**
	 * Remote Listener id.
	 */
	private final UUID rmtLsnrId;
	/**
	 * Max number of events taken from the buffer at once.
	 */
	private int evtBatchSize = 10;


	public IgniteCacheSourceTask(Map<String, String> props, Ignite igniteNode) {
		Objects.requireNonNull(igniteNode);
		sourceNode = igniteNode;
		cacheName = props.get(IgniteSourceConstants.CACHE_NAME);

		if (props.containsKey(IgniteSourceConstants.INTL_BUF_SIZE)) {
			evtBufSize = Integer.parseInt(props.get(IgniteSourceConstants.INTL_BUF_SIZE));
			evtBuf = new LinkedBlockingQueue<>(evtBufSize);
		}

		if (props.containsKey(IgniteSourceConstants.INTL_BATCH_SIZE))
			evtBatchSize = Integer.parseInt(props.get(IgniteSourceConstants.INTL_BATCH_SIZE));

		TaskRemoteFilter rmtLsnr = new TaskRemoteFilter(cacheName);
		filter = startSourceCacheListeners(props);
		try {
			rmtLsnrId = sourceNode.events(sourceNode.cluster().forCacheNodes(cacheName))
					.remoteListen(locLsnr, rmtLsnr, EventType.EVT_CACHE_OBJECT_PUT);
		} catch (Exception e) {
			log.error("Failed to register event listener!", e);
			throw new IllegalStateException(e);
		} finally {
			stopped.set(false);
		}

	}

	@SuppressWarnings("unchecked")
	private IgnitePredicate<CacheEvent> startSourceCacheListeners(Map<String, String> props) {

		IgnitePredicate<CacheEvent> ignitePredicate = null;
		if (props.containsKey(IgniteSourceConstants.CACHE_FILTER_CLASS)) {
			String filterCls = props.get(IgniteSourceConstants.CACHE_FILTER_CLASS);
			if (filterCls != null && !filterCls.isEmpty()) {
				try {
					Class<? extends IgnitePredicate<CacheEvent>> clazz =
							(Class<? extends IgnitePredicate<CacheEvent>>) Class.forName(filterCls);

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
	public List<CacheEvent> poll() {
		if (log.isDebugEnabled()) {
			log.debug("Cache source polling has been started !");
		}
		ArrayList<CacheEvent> evts = new ArrayList<>(evtBatchSize);

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
		if (rmtLsnrId != null)
			sourceNode.events(sourceNode.cluster().forCacheNodes(cacheName))
					.stopRemoteListen(rmtLsnrId);
	}

	/**
	 * Local listener buffering cache events to be further sent to Kafka.
	 */
	private static class TaskLocalListener implements IgniteBiPredicate<UUID, CacheEvent> {
		/**
		 * {@inheritDoc}
		 */
		@Override
		public boolean apply(UUID id, CacheEvent evt) {
			try {
				if (!evtBuf.offer(evt, 10, TimeUnit.MILLISECONDS))
					log.error("Failed to buffer event {}", evt.name());
			} catch (InterruptedException e) {
				log.error("Error has been thrown in TaskLocalListener {}", e);
			}

			return true;
		}
	}

	/**
	 * Remote filter.
	 */
	private static class TaskRemoteFilter implements IgnitePredicate<CacheEvent> {
		/**
		 * Cache name.
		 */
		private final String cacheName;
		/** */
		@IgniteInstanceResource
		Ignite ignite;

		/**
		 * @param cacheName Cache name.
		 */
		TaskRemoteFilter(String cacheName) {
			this.cacheName = cacheName;
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public boolean apply(CacheEvent evt) {
			Affinity<Object> affinity = ignite.affinity(cacheName);

			if (affinity.isPrimary(ignite.cluster().localNode(), evt.key()) && evt.cacheName().equals(cacheName)) {
				// Process this event. Ignored on backups.
				return filter == null || !filter.apply(evt);
			}

			return false;
		}
	}
}
