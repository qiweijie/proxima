package com.baifendian.tools.monitor;

/** 
 * 
 * @author 戚伟杰 
 * @version 2015年7月29日 下午4:11:01  
 */
public interface Settings {
	final static String host = "172.18.1.62";
	final static Integer port =7627;
	final static String topoName = "proxima";
	final static Integer defaultFrequence = 60000;
	final static String mongoHost = "172.19.1.68";
	final static Integer mongoPort = 27017;
	final static String mongoDb = "boltLog";
	final static String mongoCollection = "minutes";
}
