package kafka.examples;

import java.util.Random;

import kafka.producer.Partitioner;

public class RoutePartition implements Partitioner {
	public RoutePartition() {

	}
	@Override
	public int partition(Object arg0, int arg1) {
		Random random = new Random();
		int partition = random.nextInt(8);
		System.out.println("partition:"+partition);
		return partition;
	}

}