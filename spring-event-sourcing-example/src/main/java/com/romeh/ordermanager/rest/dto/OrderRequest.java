package com.romeh.ordermanager.rest.dto;

import java.util.Map;

import lombok.Data;

/**
 * order request json object for rest API
 *
 * @author romeh
 */
@Data
public class OrderRequest {
	private Map<String, String> orderDetails;
}
