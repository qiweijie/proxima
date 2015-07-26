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

public class KafkaConsumerProducerDemo implements KafkaProperties
{
  public static void main(String[] args)
  {
	 String zookeeper=args[0];
	 String topic = args[1];
	 int record_num= Integer.parseInt(args[2]);
	 int userNoMax = Integer.parseInt(args[3]);
	 int userNoMin = Integer.parseInt(args[4]);
	 int itemNoMax = Integer.parseInt(args[5]);
	 int itemNoMin = Integer.parseInt(args[6]);
	 
	 
    Producer producerThread = new Producer(zookeeper,topic,record_num,userNoMax,userNoMin,itemNoMax,itemNoMin);
    producerThread.start();

//    Consumer consumerThread = new Consumer(KafkaProperties.topic);
//    consumerThread.start();
    
  }
}
