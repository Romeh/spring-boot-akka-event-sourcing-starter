package com.romeh.ordermanager.reader.streamer;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * the streamer API report for how many records has been streamed and any error reporting as well
 *
 * @author romeh
 */
public class StreamReport {

	private final AtomicInteger addedRecord = new AtomicInteger(0);
	private final Set<String> errorMsgs = new HashSet<>();


	public void addErrorMsg(String errorMsg) {
		errorMsgs.add(errorMsg);
	}

	public Set<String> getErrorMsgs() {
		return errorMsgs;
	}

	public int getAddedRecords() {
		return addedRecord.get();
	}


	public void incrementAddedRecords(int count) {
		addedRecord.getAndAdd(count);
	}


	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		StreamReport that = (StreamReport) o;
		return Objects.equals(addedRecord, that.addedRecord) &&
				Objects.equals(errorMsgs, that.errorMsgs);
	}

	@Override
	public int hashCode() {
		return Objects.hash(addedRecord, errorMsgs);
	}

	@Override
	public String toString() {
		return "StreamReport{" +
				"addedRecord=" + addedRecord +
				", errorMsgs=" + errorMsgs +
				'}';
	}
}
