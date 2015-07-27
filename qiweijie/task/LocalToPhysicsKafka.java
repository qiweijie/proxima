package com.baifendian.tools.kafka;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.javaapi.producer.ProducerData;
import kafka.producer.ProducerConfig;
/** 
 * 
 * @author 戚伟杰 
 * @version 2015年7月27日 下午12:45:49  
 */
public class LocalToPhysicsKafka extends Thread {
	public static void main(String[] args){
		String topic;
		String filename;
		String line=null;
		Integer count = 0;
//		获取参数，topic，filename
		try {
			 topic = args[0];
			 filename = args[1];
		} catch (Exception e) {
//			使用默认配置
			e.printStackTrace();
			 topic = "qwj-upload";
			 filename = "result.txt";
		}
//		连接目标kafka
		Properties props = new Properties();		
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("zk.connect", "172.18.1.62:2181");
        Producer<String, String> producer = new Producer<String, String>(new ProducerConfig(props));
        
//		打开文件，每次读取一行
//		File data = new File(filename);
        System.out.println("begin reading data from file:"+filename);
		try {
			BufferedReader br = new BufferedReader(new FileReader(filename));
			try {
				while(( line= br.readLine())!=null){
//					发送给kafka
					producer.send(new ProducerData(topic, line));
		            System.out.println("Send:" + line+".aready send "+count+"message!");
					count++;
		        	try {
						sleep(100);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
				br.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
