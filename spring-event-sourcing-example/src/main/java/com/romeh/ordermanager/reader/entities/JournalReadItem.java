package com.romeh.ordermanager.reader.entities;

import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryReader;
import org.apache.ignite.binary.BinaryWriter;
import org.apache.ignite.binary.Binarylizable;
import org.apache.ignite.cache.query.annotations.QuerySqlField;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Created by MRomeh
 * the journal read side cache value object
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class JournalReadItem implements Binarylizable {

	public static final String ORDER_ID = "orderId";
	public static final String STATUS_FIELD = "status";


	@QuerySqlField(index = true)
	private String orderId;
	@QuerySqlField(index = true)
	private String status;


	@Override
	public void writeBinary(BinaryWriter out) throws BinaryObjectException {
		out.writeString(ORDER_ID, orderId);
		out.writeString(STATUS_FIELD, status);
	}

	@Override
	public void readBinary(BinaryReader in) throws BinaryObjectException {
		orderId = in.readString(ORDER_ID);
		status = in.readString(STATUS_FIELD);
	}
}