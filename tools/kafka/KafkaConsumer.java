package com.baifendian.tools.kafka;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

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
public class KafkaConsumer extends Thread
{
    private final ConsumerConnector consumer;
    private final String topic;

    public KafkaConsumer(String topic)
    {
        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(
                createConsumerConfig());
        this.topic = topic;
    }

    private static ConsumerConfig createConsumerConfig()
    {
        Properties props = new Properties();
        props.put("zk.connect", KafkaProperties.zkConnect);
        props.put("groupid", KafkaProperties.groupId);
        props.put("zk.sessiontimeout.ms", "400");
        props.put("zk.synctime.ms", "200");
        props.put("autocommit.interval.ms", "1000");
        return new ConsumerConfig(props);
    }

    @Override
    public void run() {
        System.out.println("begin receive：");
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, new Integer(1));
        Map<String, List<KafkaStream<Message>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        KafkaStream<Message> stream = consumerMap.get(topic).get(0);
        ConsumerIterator<Message> it = stream.iterator();
        while (it.hasNext()) {
        	MessageAndMetadata<Message> message = it.next();
            ByteBuffer buffer = message.message().payload();
            byte [] bytes = new byte[buffer.remaining()];
            buffer.get(bytes);
            System.out.println("receive："+new String(bytes)+". it's topic is "+message.topic()+". it's offset is ");
            try {
                sleep(300);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
