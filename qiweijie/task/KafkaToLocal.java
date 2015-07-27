package com.baifendian.tools.kafka;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.Message;
import kafka.message.MessageAndMetadata;
/** 
 * 
 * @author 戚伟杰 
 * @version 2015年7月27日 下午12:45:49  
 */
public class KafkaToLocal extends Thread{
	public static void main(String[] args){
		String topic;
		String filename;
		Integer index;
		Integer num;
//		获取参数，topic，filename，index，多少条开始存到本地
		try {
			 topic = args[0];
			 filename = args[1];
			 index = Integer.parseInt(args[2]);
			 num = Integer.parseInt(args[3]);
		} catch (Exception e) {
//			使用默认配置
			e.printStackTrace();
			 topic = "qwj-task";
			 filename = "result.txt";
			 index = 0;
			 num = 100;
		}
		Integer count = 0;
		List<String> content=new ArrayList<String>();
//		连接kafka
        Properties props = new Properties();
        props.put("zk.connect", KafkaProperties.zkConnect);
        props.put("groupid", KafkaProperties.groupId);
        props.put("zk.sessiontimeout.ms", "400");
        props.put("zk.synctime.ms", "200");
        props.put("autocommit.interval.ms", "1000");
        ConsumerConnector consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(props));
//    开始接受消息 
        System.out.println("begin receive：");
        Map<String,Integer> topicCountMap = new HashMap<String,Integer>();
        topicCountMap.put(topic, new Integer(1));
        Map<String,List<KafkaStream<Message>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        KafkaStream<Message> stream = consumerMap.get(topic).get(index);
        ConsumerIterator<Message> it = stream.iterator();
        while(it.hasNext()){
        	MessageAndMetadata<Message> message = it.next();
        	ByteBuffer buffer = message.message().payload();
        	byte [] bytes = new byte[buffer.remaining()];
        	buffer.get(bytes);
        	String msg  = new String(bytes);
        	System.out.println("recieve:"+msg+". it's topic is "+message.topic()+". index is "+index+". count is"+count);
//        	判断消息是否符合条件
        	Boolean ok = CheckMessage(msg);
        	if(ok){
//        		如果符合，计数器加1，并放到content里面
        		count++;
        		content.add(msg);
        	}
        	if(count>=num){
//        		达到最大次数后，一次性写入本机文件
        		WriteToLocal(filename,content);
        		content.clear();
        		count=0;
        	}
        	try {
				sleep(300);
			} catch (Exception e) {
				e.printStackTrace();
			}
        }
//        把最后的消息写入本地文件
		WriteToLocal(filename,content);
	}

	private static void WriteToLocal(String filename,List<String> content) {
		// TODO Auto-generated method stub
		try {
			FileWriter writer = new FileWriter(filename,true);
			BufferedWriter bw = new BufferedWriter(writer); 
			for(String message:content){
				bw.write(message);
				bw.newLine();
			}
			if(writer!=null){
				writer.close();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private static Boolean CheckMessage(String msg) {
		// TODO Auto-generated method stub
		return true;
	}
}
