package com.ylc.log;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class AppLog {
	//log4j configuration under src/main/resources/log4j2.xml
	public static final Logger log = LogManager.getLogger(AppLog.class);

}