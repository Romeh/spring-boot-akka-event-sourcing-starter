package com.romeh.ordermanager.reader.streamer.grid.streamer;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.cache.CacheException;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.romeh.ordermanager.reader.streamer.IgniteSink;
import com.romeh.ordermanager.reader.streamer.IgniteSource;
import com.romeh.ordermanager.reader.streamer.StreamReport;
import com.romeh.ordermanager.reader.streamer.grid.cq.IgniteCacheSinkTaskCQ;
import com.romeh.ordermanager.reader.streamer.grid.cq.IgniteCacheSourceTaskCQ;
import com.romeh.ordermanager.reader.streamer.grid.events.IgniteCacheSinkTask;
import com.romeh.ordermanager.reader.streamer.grid.events.IgniteCacheSourceTask;

import io.reactivex.Flowable;
import io.reactivex.functions.Consumer;
import io.reactivex.functions.Predicate;

/**
 * the main cache data event streamer between source cache and sink cache
 *
 * @author romeh
 */
public final class IgniteCacheEventStreamerRx<T> {
	/**
	 * Logger.
	 */
	private static final Logger log = LoggerFactory.getLogger(IgniteCacheEventStreamerRx.class);
	private static final Predicate<Throwable> retryPredicate = throwable ->
			throwable instanceof CacheException || throwable instanceof IgniteException;
	private static final Consumer<Throwable> onErrorAction = throwable ->
			log.error("error has thrown in the final stage of the flow {}", throwable.getMessage());
	private final IgniteSource igniteCacheSourceTask;
	private final IgniteSink igniteCacheSinkTask;
	private final long periodicInterval;
	private final long retryTimes;
	private final int flushBufferSize;
	private final AtomicInteger atomicInteger = new AtomicInteger(0);
	private final StreamReport streamReport = new StreamReport();
	private final Consumer<Collection> onNextAction;

	/**
	 * @param igniteCacheSourceTask ignite source cache task
	 * @param igniteCacheSinkTask   ignite sink cache task
	 * @param retryTimes            how many times to retry in case of error
	 * @param flushBufferSize       flush buffer size in the streamer
	 * @param periodicInterval      the polling interval
	 */
	private IgniteCacheEventStreamerRx(IgniteSource<T> igniteCacheSourceTask, IgniteSink<T> igniteCacheSinkTask, long retryTimes, int flushBufferSize, long periodicInterval) {
		this.igniteCacheSourceTask = igniteCacheSourceTask;
		this.igniteCacheSinkTask = igniteCacheSinkTask;
		if (retryTimes != 0L) {
			this.retryTimes = retryTimes;
		} else {
			this.retryTimes = 2;
		}
		if (flushBufferSize != 0) {
			this.flushBufferSize = flushBufferSize;
		} else {
			this.flushBufferSize = 10;
		}
		if (periodicInterval != 0) {
			this.periodicInterval = periodicInterval;
		} else {
			this.periodicInterval = 500L;
		}
		this.onNextAction = cacheEvents -> {
			igniteCacheSinkTask.flush();
			streamReport.incrementAddedRecords(cacheEvents.size());
		};
	}

	/**
	 * @return streamer using Ignite cache continuous query
	 */
	public static BuilderCq builderWithContinuousQuery() {
		return new BuilderCq();
	}

	/**
	 * @return streamer using Ignite events streaming
	 */
	public static BuilderWithSystemEvents builderWithGridEvents() {
		return new BuilderWithSystemEvents();
	}

	/**
	 * @return the streamer execution report
	 */
	public final StreamReport getExecutionReport() {
		return streamReport;
	}

	/**
	 * start and execute the streamer flow
	 */
	@SuppressWarnings("unchecked")
	public final void execute() {

		log.info("starting the stream for the ignite source/sink flow");

		if (igniteCacheSourceTask.isStopped()) throw new IllegalStateException("Ignite source task is not yet started");
		if (igniteCacheSinkTask.isStopped()) throw new IllegalStateException("Ignite sink task is not yet started");

		//noinspection unchecked
		Flowable.fromCallable(igniteCacheSourceTask::poll)
				.repeatWhen(flowable -> flowable.delay(periodicInterval, TimeUnit.MILLISECONDS))
				.retry(retryTimes, retryPredicate)
				.doOnError(throwable -> streamReport.addErrorMsg(throwable.getMessage()))
				.doOnNext(igniteCacheSinkTask::put)
				.doOnError(throwable -> streamReport.addErrorMsg(throwable.getMessage()))
				.doOnNext(data -> {
					if (null != data && !data.isEmpty() && atomicInteger.addAndGet(data.size()) % flushBufferSize == 0) {
						igniteCacheSinkTask.flush();
					}
				})
				.retry(retryTimes, retryPredicate)
				.doOnError(throwable -> streamReport.addErrorMsg(throwable.getMessage()))
				.doFinally(() -> {
					log.info("cleaning and stopping ignite tasks from the stream");
					if (log.isDebugEnabled()) {
						log.debug("final execution report: error messages : {}, Total streamed record number : {}", streamReport.getErrorMsgs().toArray(), streamReport.getAddedRecords());
					}
					igniteCacheSinkTask.stop();
					igniteCacheSourceTask.stop();
				})
				.subscribe(onNextAction, onErrorAction);
	}

	/**
	 * the ignite continuous query streamer builder
	 */
	public static final class BuilderCq {
		private IgniteCacheSourceTaskCQ igniteCacheSourceTask;
		private IgniteCacheSinkTaskCQ igniteCacheSinkTask;
		private long retryTimesOnError;
		private int flushBufferSize;
		private long pollingInterval;

		private BuilderCq() {
		}

		public BuilderCq sourceCache(Map<String, String> props, Ignite sourceNode) {
			igniteCacheSourceTask = new IgniteCacheSourceTaskCQ(props, sourceNode);
			return this;
		}

		public BuilderCq sinkCache(Map<String, String> props, Ignite sinkNode) {
			igniteCacheSinkTask = new IgniteCacheSinkTaskCQ(props, sinkNode);
			return this;
		}

		public BuilderCq retryTimesOnError(long retryTimesOnError) {
			this.retryTimesOnError = retryTimesOnError;
			return this;
		}

		public BuilderCq flushBufferSize(int flushBufferSize) {
			this.flushBufferSize = flushBufferSize;
			return this;
		}

		public BuilderCq pollingInterval(int pollingInterval) {
			this.pollingInterval = pollingInterval;
			return this;
		}

		@SuppressWarnings("unchecked")
		public IgniteCacheEventStreamerRx build() {
			return new IgniteCacheEventStreamerRx(igniteCacheSourceTask, igniteCacheSinkTask, retryTimesOnError, flushBufferSize, pollingInterval);
		}

	}

	/**
	 * the ignite events streamer builder
	 */
	public static final class BuilderWithSystemEvents {
		private IgniteCacheSourceTask igniteCacheSourceTask;
		private IgniteCacheSinkTask igniteCacheSinkTask;
		private long retryTimesOnError;
		private int flushBufferSize;
		private long pollingInterval;

		private BuilderWithSystemEvents() {
		}

		public BuilderWithSystemEvents sourceCache(Map<String, String> props, Ignite sourceNode) {
			igniteCacheSourceTask = new IgniteCacheSourceTask(props, sourceNode);
			return this;
		}

		public BuilderWithSystemEvents sinkCache(Map<String, String> props, Ignite sinkNode) {
			igniteCacheSinkTask = new IgniteCacheSinkTask(props, sinkNode);
			return this;
		}

		public BuilderWithSystemEvents retryTimesOnError(long retryTimesOnError) {
			this.retryTimesOnError = retryTimesOnError;
			return this;
		}

		public BuilderWithSystemEvents flushBufferSize(int flushBufferSize) {
			this.flushBufferSize = flushBufferSize;
			return this;
		}

		public BuilderWithSystemEvents pollingInterval(int pollingInterval) {
			this.pollingInterval = pollingInterval;
			return this;
		}

		@SuppressWarnings("unchecked")
		public IgniteCacheEventStreamerRx build() {
			return new IgniteCacheEventStreamerRx(igniteCacheSourceTask, igniteCacheSinkTask, retryTimesOnError, flushBufferSize, pollingInterval);
		}

	}

}
