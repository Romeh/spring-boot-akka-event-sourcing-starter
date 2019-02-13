package com.romeh.ordermanager.reader.streamer;

import java.util.Collection;

/**
 * the generic Ignite sink task interface to be used into the streamer flow
 *
 * @author romeh
 */
public interface IgniteSink<T> {

	/**
	 * @param records the records to stream to the sink cache
	 */
	void put(Collection<T> records);

	/**
	 * stop the sink streamer
	 */
	void stop();

	/**
	 * flush the buffered records in the sink streamer
	 */
	void flush();

	/**
	 * @return boolean of if the sink streamer is already stopped
	 */
	boolean isStopped();
}
