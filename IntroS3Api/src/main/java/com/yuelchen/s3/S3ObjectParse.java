package com.yuelchen.s3;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;

public class S3ObjectParse {
	
	/**
	 * The default S3 object prefix delimiter.
	 */
	public static final String PREFIX_DELIMITER = "/";
	
	//====================================================================================================
	
	private S3ObjectParse() {}
	
	//====================================================================================================
	
	public static String getFilename(String objectPrefix) {
		return objectPrefix.contains(PREFIX_DELIMITER) ?
				objectPrefix.substring(objectPrefix.lastIndexOf(PREFIX_DELIMITER) + 1) 
				: objectPrefix;
	}
	
	//====================================================================================================
	
	public static String getBaseName(String objectPrefix) {
		return objectPrefix.contains(PREFIX_DELIMITER) ?
				objectPrefix.substring(0, objectPrefix.lastIndexOf(PREFIX_DELIMITER) + 1)
				: "";
	}
	
	//====================================================================================================
	
	public static String getBasePath(String objectPrefix) {
		return objectPrefix.contains(PREFIX_DELIMITER) ?
				objectPrefix.substring(0, objectPrefix.lastIndexOf(PREFIX_DELIMITER))
				: "";
	}
	
	//====================================================================================================
	
	public static String decodeAsUTF8(String objectPrefix) 
			throws UnsupportedEncodingException {
		
		return URLDecoder.decode(objectPrefix, StandardCharsets.UTF_8.name());
	}
	
	//====================================================================================================
	
	public static String decodeAsASCII(String objectPrefix) 
			throws UnsupportedEncodingException {
		
		return URLDecoder.decode(objectPrefix, StandardCharsets.US_ASCII.name());
	}
}
