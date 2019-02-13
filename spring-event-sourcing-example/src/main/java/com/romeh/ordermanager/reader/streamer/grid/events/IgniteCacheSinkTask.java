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

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.events.CacheEvent;
import org.apache.ignite.stream.StreamSingleTupleExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.romeh.ordermanager.reader.streamer.IgniteSink;
import com.romeh.ordermanager.reader.streamer.IgniteSinkConstants;

/**
 * Task to consume sequences of SinkRecords generated from ignite events stream and write data to grid.
 */
public class IgniteCacheSinkTask implements IgniteSink<CacheEvent> {
	/**
	 * Logger.
	 */
	private static final Logger log = LoggerFactory.getLogger(IgniteCacheSinkTask.class);
	/**
	 * Flag for stopped state.
	 */
	private static volatile boolean stopped = true;
	/**
	 * Cache name.
	 */
	private static String cacheName;
	private static StreamerContext streamerContext;
	/**
	 * Entry transformer.
	 */
	private final StreamSingleTupleExtractor<CacheEvent, Object, Object> extractor;


	public IgniteCacheSinkTask(Map<String, String> props, Ignite sinkNode) {
		Objects.requireNonNull(sinkNode);
		cacheName = Optional.ofNullable(props.get(IgniteSinkConstants.CACHE_NAME))
				.orElseThrow(() -> new IllegalArgumentException("Cache name in sink task can not be NULL !"));

		streamerContext = new StreamerContext(sinkNode);

		if (props.containsKey(IgniteSinkConstants.CACHE_ALLOW_OVERWRITE))
			streamerContext.getStreamer().allowOverwrite(
					Boolean.parseBoolean(props.get(IgniteSinkConstants.CACHE_ALLOW_OVERWRITE)));

		if (props.containsKey(IgniteSinkConstants.CACHE_PER_NODE_DATA_SIZE))
			streamerContext.getStreamer().perNodeBufferSize(
					Integer.parseInt(props.get(IgniteSinkConstants.CACHE_PER_NODE_DATA_SIZE)));

		if (props.containsKey(IgniteSinkConstants.CACHE_PER_NODE_PAR_OPS))
			streamerContext.getStreamer().perNodeParallelOperations(
					Integer.parseInt(props.get(IgniteSinkConstants.CACHE_PER_NODE_PAR_OPS)));


		extractor = initTransformer(props);

		stopped = false;

	}

	@SuppressWarnings("unchecked")
	private StreamSingleTupleExtractor<CacheEvent, Object, Object> initTransformer(Map<String, String> props) {
		StreamSingleTupleExtractor<CacheEvent, Object, Object> extractor = null;
		if (props.containsKey(IgniteSinkConstants.SINGLE_TUPLE_EXTRACTOR_CLASS)) {
			String transformerCls = props.get(IgniteSinkConstants.SINGLE_TUPLE_EXTRACTOR_CLASS);
			if (transformerCls != null && !transformerCls.isEmpty()) {
				try {
					Class<? extends StreamSingleTupleExtractor<CacheEvent, Object, Object>> clazz =
							(Class<? extends StreamSingleTupleExtractor<CacheEvent, Object, Object>>)
									Class.forName(transformerCls);

					extractor = clazz.newInstance();
				} catch (Exception e) {
					throw new IllegalStateException("Failed to instantiate the provided transformer!", e);
				}
			}
		}
		return extractor;

	}


	public boolean isStopped() {
		return stopped;
	}

	/**
	 * Buffers records.
	 *
	 * @param records Records to inject into grid.
	 */
	@SuppressWarnings("unchecked")
	@Override
	public void put(Collection<CacheEvent> records) {
		if (log.isDebugEnabled()) {
			log.debug("Sink cache put : {}", records);
		}
		if (null != records && !records.isEmpty()) {
			for (CacheEvent record : records) {
				// Data is flushed asynchronously when CACHE_PER_NODE_DATA_SIZE is reached.
				if (extractor != null) {
					Map.Entry<Object, Object> entry = extractor.extract(record);
					if (null != entry) {
						streamerContext.getStreamer().addData(entry.getKey(), entry.getValue());
					}
				} else {
					if (record.key() != null) {
						streamerContext.getStreamer().addData(record.key(), record.hasNewValue() ? record.newValue() : record.oldValue());
					} else {
						log.error("Failed to stream a record with null key!");
					}
				}
			}
		}

	}

	/**
	 * Pushes buffered data to grid. Flush interval is configured by worker configurations.
	 */
	@Override
	public void flush() {
		if (log.isDebugEnabled()) {
			log.debug("Sink Cache flush is called");
		}
		if (stopped)
			return;

		streamerContext.getStreamer().flush();
	}

	/**
	 * Stops the grid client.
	 */
	@Override
	public void stop() {
		if (stopped)
			return;

		stopped = true;
		streamerContext.getStreamer().close();
	}


	/**
	 * Streamer context initializing grid and data streamer instances on demand.
	 */
	private static class StreamerContext {
		private final Ignite ignite;
		private final IgniteDataStreamer streamer;

		/**
		 * Constructor.
		 *
		 * @param ignite
		 */
		StreamerContext(Ignite ignite) {
			this.ignite = ignite;
			streamer = this.ignite.dataStreamer(cacheName);
		}


		/**
		 * Obtains grid instance.
		 *
		 * @return Grid instance.
		 */
		public Ignite getIgnite() {
			return ignite;
		}

		/**
		 * Obtains data streamer instance.
		 *
		 * @return Data streamer instance.
		 */
		IgniteDataStreamer getStreamer() {
			return streamer;
		}
	}
}
