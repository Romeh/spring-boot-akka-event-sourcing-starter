package com.romeh.ordermanager.reader.streamer;

import java.util.Collection;


/**
 * the generic interface for ignite source definition which can be used into the streamer API
 *
 * @author romeh
 */
public interface IgniteSource<T> {

	/**
	 * @return the records which have been polled from the source cache
	 */
	Collection<T> poll();

	/**
	 * stop the source cache task
	 */
	void stop();

	/**
	 * @return if the source cache task is stopped
	 */
	boolean isStopped();
}
