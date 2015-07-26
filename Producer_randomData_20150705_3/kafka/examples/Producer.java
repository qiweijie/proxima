/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.examples;


import java.util.Date;
import java.util.List;
import java.util.Properties;

import kafka.javaapi.producer.ProducerData;
import kafka.producer.ProducerConfig;


public class Producer extends Thread
{
  private kafka.javaapi.producer.Producer producer;
  private final String topic;
  private Properties props = new Properties();
  private  int record_num;
	//user range
	 int userNoMax =2000000;
	 int userNoMin =1000000;
	
	 //item range
	 int itemNoMax =20000;
	 int itemNoMin =10000;
  
  public Producer(String topic)
  {
    props.put("serializer.class", "kafka.serializer.StringEncoder");
    props.put("zk.connect", "172.18.1.62:2181");
//    props.put(key, value)
    // Use random partitioner. Don't need the key type. Just set it to Integer.
    // The message is of type String.
    producer = new kafka.javaapi.producer.Producer<Integer, String>(new ProducerConfig(props));
    this.topic = topic;
  }
  
  public Producer(String zookeeper_server,String topic,int record_num,int userNoMax,int userNoMin,int itemNoMax,int itemNoMin)
  {
    props.put("serializer.class", "kafka.serializer.StringEncoder");
    props.put("zk.connect", zookeeper_server);
    props.put("partitioner.class","kafka.examples.RoutePartition");
    //props.put("zk.connect", "172.18.1.62:2181");
    // Use random partitioner. Don't need the key type. Just set it to Integer.
    // The message is of type String.
    producer = new kafka.javaapi.producer.Producer(new ProducerConfig(props));
    this.topic = topic;
    
    this.record_num=record_num;
    this.userNoMax= userNoMax;
    this.userNoMin = userNoMin;
    this.itemNoMax = itemNoMax;
    this.itemNoMin = itemNoMin;
  }
  

  
  public void run() {
	  	List messages = new java.util.ArrayList();
		Date date = new Date();
		String start_time = date.toString();
		Long start_second = System.currentTimeMillis();

		//1.循环生成10000记录 10个商品  1000用户
		//生成一个8位数据，将这个8位数分20%   80%


		 StringBuilder stringBuilder = new StringBuilder();
		 int j=0;
		 for(int i=1;i<=record_num;i++){
//		 for(int i=1;i<=100;i++){
			String cid=random_data.getCid();
			String uid=String.valueOf(random_data.getLongTailNum(userNoMax,userNoMin));
			String iid=(cid.equals("haoleimai")?"H":"Z")+String.valueOf(random_data.getLongTailNum(itemNoMax,userNoMin)); 
		    String method=random_data.getMethod();
		    
		    Long timestamp =System.currentTimeMillis();
		    /* stringBuilder.append(cid);
		    stringBuilder.append(",");
		    stringBuilder.append(iid);
		    stringBuilder.append(",");
		    stringBuilder.append(method);
		    stringBuilder.append(",");
		    stringBuilder.append(timestamp);
		    stringBuilder.append(",");
		    stringBuilder.append(uid);*/
		    stringBuilder.append("{DS.Input.All.G");
		    stringBuilder.append(cid);
		    stringBuilder.append("}{\"cid\":\"C");
		    stringBuilder.append(cid);
		    stringBuilder.append("\",\"iid\":\"");
		    stringBuilder.append(iid);
		    stringBuilder.append("\",\"method\":\"");
		    stringBuilder.append(method);
		    stringBuilder.append("\",\"timestamp\":\"");
		    stringBuilder.append(timestamp.toString());
		    stringBuilder.append("\",\"uid\":\"");
		    stringBuilder.append(uid);
		    stringBuilder.append("\"}");  
		    
            messages.add(stringBuilder.toString());
//		    System.out.println(stringBuilder.toString());
		    if(i%1000000==0){//每10W条输出一次     
		    	System.out.println("随机生成数据第"+i+"条；");
		    }
		    
		    j++;
		    if(j==10000){
		    	producer.send(new ProducerData(topic, "keypartition", messages));
		    	j=0;
		    	messages=null;
		    	messages= new java.util.ArrayList();
		    	System.out.println("10000 messages send");
		    }
		    stringBuilder = new StringBuilder();
		    
		}
		 if(messages!=null&&messages.size()>0)
			 producer.send(new ProducerData(topic, "keypartition", messages));
		 date = new Date();
		 Long end_second = System.currentTimeMillis();
		 double spent_minute = (end_second-start_second)/1000.0/60.0;
		 String done_time= date.toString();
		 System.out.println("output done!start_time="+start_time+";done_time="+done_time+";spent_minute="+spent_minute);
     
  }

}
