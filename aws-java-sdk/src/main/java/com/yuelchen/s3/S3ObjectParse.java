package com.yuelchen.s3;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;

/**
 * S3ObjectParse if for handling object prefix related operations. 
 * 
 * @author 	yuelchen
 * @version	1.0.0
 * @since 	2019-09-01
 */
public class S3ObjectParse {
	
	/**
	 * The default S3 object prefix delimiter.
	 */
	public static final String PREFIX_DELIMITER = "/";
	
	//====================================================================================================
	
	/**
	 * Private constructor
	 */
	private S3ObjectParse() {}
	
	//====================================================================================================
	
	/**
	 * Returns the filename for given object prefix. 
	 * 
	 * @param objectPrefix						the object prefix. 
	 * 
	 * @return									the filename. 
	 */
	public static String getFilename(String objectPrefix) {
		return objectPrefix.contains(PREFIX_DELIMITER) ?
				objectPrefix.substring(objectPrefix.lastIndexOf(PREFIX_DELIMITER) + 1) 
				: objectPrefix;
	}
	
	//====================================================================================================
	
	/**
	 * Return the base name for given object prefix. 
	 * i.e. /land/data/raw/sensitive/filename.txt will return /land/data/raw/sensitive.
	 * 
	 * 
	 * @param objectPrefix						the object prefix. 
	 * 
	 * @return									the base name. 
	 */
	public static String getBaseName(String objectPrefix) {
		return objectPrefix.contains(PREFIX_DELIMITER) ?
				objectPrefix.substring(0, objectPrefix.lastIndexOf(PREFIX_DELIMITER))
				: "";
	}
	
	//====================================================================================================
	
	/**
	 * Returns the base path for given object prefix. 
	 * i.e. /land/data/raw/sensitive/filename.txt will return /land/data/raw/sensitive/.
	 * 
	 * @param objectPrefix						the object prefix. 
	 * 
	 * @return									the base path. 
	 */
	public static String getBasePath(String objectPrefix) {
		return objectPrefix.contains(PREFIX_DELIMITER) ?
				objectPrefix.substring(0, objectPrefix.lastIndexOf(PREFIX_DELIMITER) + 1)
				: "";
	}
	
	//====================================================================================================
	
	/**
	 * Returns an object prefix of UTF-8 encoding. 
	 * 
	 * @param objectPrefix						the object prefix. 
	 * 
	 * @return									object prefix of UTF-8 encoding. 
	 * 
	 * @throws UnsupportedEncodingException		thrown when encoding is not supported.  
	 */
	public static String decodeAsUTF8(String objectPrefix) 
			throws UnsupportedEncodingException {
		
		return URLDecoder.decode(objectPrefix, StandardCharsets.UTF_8.name());
	}
	
	//====================================================================================================
	
	/**
	 * Returns an object prefix of ASCII encoding. 
	 * 
	 * @param objectPrefix						the object prefix. 
	 * 
	 * @return									object prefix of ASCII encoding. 
	 * 
	 * @throws UnsupportedEncodingException		thrown when encoding is not supported.  
	 */
	public static String decodeAsASCII(String objectPrefix) 
			throws UnsupportedEncodingException {
		
		return URLDecoder.decode(objectPrefix, StandardCharsets.US_ASCII.name());
	}
}
