package com.baifendian.tools.kafka;
/** 
 * 
 * @author 戚伟杰 
 * @version 2015年7月27日 下午12:45:49  
 */

public class KafkaConsumerProducerDemo
{
    public static void main(String[] args)
    {
//        KafkaProducer producerThread = new KafkaProducer(KafkaProperties.topic);
//        producerThread.start();

        KafkaConsumer consumerThread = new KafkaConsumer(KafkaProperties.topic);
        consumerThread.start();
    }
}