package com.reavely.openrtb;

import org.apache.avro.generic.GenericRecord;

/**
 * 
 * @author Simon Reavely
 *
 */
public interface AvroRequestProcessor {
	public GenericRecord processRequest(GenericRecord request) throws Exception;

}
