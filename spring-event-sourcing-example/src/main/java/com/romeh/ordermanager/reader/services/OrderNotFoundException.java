package com.romeh.ordermanager.reader.services;

/**
 * order not found exception
 *
 * @author romeh
 */
public class OrderNotFoundException extends Exception {

	public OrderNotFoundException(String errorMsg) {
		super(errorMsg);
	}
}
