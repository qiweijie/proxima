package com.baifendian.tools.monitor;

/** 
 * 
 * @author 戚伟杰 
 * @version 2015年7月29日 下午8:18:32  
 */
public class MonitorDemo {
	public static void main(String[] args){
		Monitor monitor = new Monitor(Settings.host, 
				Settings.port, Settings.topoName, Settings.defaultFrequence);
		monitor.start();
	}
}
