package com.baifendian.tools.monitor;

import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.thrift7.TException;
import org.apache.thrift7.protocol.TBinaryProtocol;
import org.apache.thrift7.transport.TFramedTransport;
import org.apache.thrift7.transport.TSocket;
import org.apache.thrift7.transport.TTransportException;
import org.json.simple.JSONObject;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;

import backtype.storm.generated.Bolt;
import backtype.storm.generated.GlobalStreamId;
import backtype.storm.generated.Nimbus.Client;
import backtype.storm.generated.NotAliveException;
import backtype.storm.generated.StormTopology;
import backtype.storm.generated.TaskStats;
import backtype.storm.generated.TaskSummary;
import backtype.storm.generated.TopologyInfo;
import backtype.storm.generated.WorkerSummary;

/** 
 * 
 * @author 戚伟杰 
 * @version 2015年7月29日 下午4:04:53  
 */
public class Monitor extends Thread {
	private final String topoName;
	private final Integer frequence;
	private final String host;
	private final Integer port;
	public  Monitor(String host,Integer port,String topoName,Integer frequence){
		this.topoName = topoName;
		this.frequence = frequence;
		this.host = host;
		this.port = port;
	}
	@Override
	public void run() {
		// TODO Auto-generated method stub
//		获取client
		TSocket tSocket = new TSocket(this.host, this.port);
		TFramedTransport tFramedTransport = new TFramedTransport(tSocket);
		TBinaryProtocol tBinaryProtocol = new TBinaryProtocol(tFramedTransport);
		Client client = new Client(tBinaryProtocol);
		try {
			tFramedTransport.open();
			while(true){
				try {
	//			从stormTopology里面获取bolt信息，需要先从topoInfo里面获取id
					TopologyInfo topologyInfo = client.getTopologyInfoByName(this.topoName);
					StormTopology stormTopology = client.getTopology(topologyInfo.get_id());
	//				获取bolt
					Map<String,Bolt> bolts = stormTopology.get_bolts();
					List<String> boltList = new ArrayList<String>();
					for(String key:bolts.keySet()){
						if(!key.startsWith("__")){
	//						System.out.println(key);
							boltList.add(key);
						}
					}
	//				获取task信息
					List<WorkerSummary> workerSummary = topologyInfo.get_workers();
	//				把tasks按照bolt-tasks对应好
					Map<String, List<TaskSummary>> classifiedData = Classify(boltList,workerSummary);
	//				对每个bolt的tasks进行处理处理
					Map<String, Number> result=Compute(classifiedData);
	//				把结果写入mongo数据库，以时间为键，值为一个字典（键为bolt名字，值为数据）
					WriteToDatabase( result);
					try {
						sleep(this.frequence);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				} catch (NotAliveException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (TException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		} catch (TTransportException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		super.run();
	}
private Map<String, Number> Compute(Map<String, List<TaskSummary>> classifiedData) {
		// TODO Auto-generated method stub
		Long total_emitted = (long) 0;
		Double total_send = (double) 0;
		Double total_recv = (double) 0;
		Long total_acked = (long) 0;
		Double total_process = (double) 0;
		Long total_failed = (long) 0;
		Integer total_tasks = 0;
		for(String bolt:classifiedData.keySet()){
			List<TaskSummary> temp = classifiedData.get(bolt);
			for(TaskSummary ts:temp){
//				System.out.println("ts");
				total_tasks++;
				Map<String,Number> temp1 = ComputeTask(ts);
				total_emitted+=temp1.get("total_emitted").longValue();
				total_send+=temp1.get("total_send").doubleValue();
				total_recv+=temp1.get("total_recv").doubleValue();
				total_acked+=temp1.get("total_acked").longValue();
				total_process+=temp1.get("total_process").doubleValue();
				total_failed+=temp1.get("total_failed").longValue();
			}
		}
		Map<String,Number> result=new TreeMap<String, Number>();
		result.put("total_emitted", total_emitted);
		result.put("total_send", total_send);
		result.put("total_recv", total_recv);
		result.put("total_acked", total_acked);
		result.put("total_process", total_process/total_tasks);
		result.put("total_failed", total_failed);
		return result;
}
private void WriteToDatabase(Map<String, Number> result) {
	// TODO Auto-generated method stub
	System.out.println("\tthis bolt's result showed as followed:\r\n\t"+result);
//	获取当前时间
	SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	String now = df.format(new Date());
	try {
//		MongoClient mongo = new MongoClient(Settings.mongoHost,Settings.mongoPort);
		MongoClient mongo = new MongoClient(Settings.mongoHost,Settings.mongoPort);
		DB db = mongo.getDB(Settings.mongoDb);
		DBCollection collection = db.getCollection(Settings.mongoCollection);
		BasicDBObject document = new BasicDBObject();
		document.put(now, JSONObject.toJSONString(result));
		collection.insert(document);
		System.out.println("insert success!");
//        // 创建要查询的document
//        BasicDBObject searchQuery = new BasicDBObject();
//        searchQuery.put("id", 1001);
//        // 使用collection的find方法查找document
//        DBCursor cursor = collection.find(searchQuery);
//        //循环输出结果
//        while (cursor.hasNext()) {
//        System.out.println(cursor.next());
//        }
	} catch (UnknownHostException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
//	System.out.println(JSONObject.toJSONString(result));
}
// 计算每一个task，返回一个字典
@SuppressWarnings("null")
private Map<String, Number> ComputeTask(TaskSummary ts) {
// TODO Auto-generated method stub
	Map<String, Long> emitted = ts.get_stats().get_emitted().get("600");
	Long total_emitted = (long) 0;
	for(String component_id:emitted.keySet()){
		total_emitted+=emitted.get(component_id);
	}
	Map<String, Double> send_tps = ts.get_stats().get_send_tps().get("600");
	Double total_send = (double) 0;
	for(String component_id:send_tps.keySet()){
		total_send+=send_tps.get(component_id);
	}
	Map<String, Double> recv_tps = ts.get_stats().get_send_tps().get("600");
	Double total_recv = (double) 0;
	for(String component_id:recv_tps.keySet()){
		total_recv+=recv_tps.get(component_id);
	}
	Map<GlobalStreamId, Long> acked = ts.get_stats().get_acked().get("600");
	Long total_acked = (long) 0;
	for(GlobalStreamId GlobalStreamId_id:acked.keySet()){
		total_acked+=acked.get(GlobalStreamId_id);
	}
	Map<GlobalStreamId, Double> process = ts.get_stats().get_process_ms_avg().get("600");
	Double total_process = (double) 0;
	for(GlobalStreamId GlobalStreamId_id:process.keySet()){
		total_process+=process.get(GlobalStreamId_id);
	}
	Map<GlobalStreamId, Long> failed = ts.get_stats().get_failed().get("10800");
	Long total_failed = (long) 0;
	for(GlobalStreamId GlobalStreamId_id:failed.keySet()){
		total_failed+=failed.get(GlobalStreamId_id);
	}
	Map<String,Number> result=new TreeMap<String, Number>();
	result.put("total_emitted", total_emitted);
	result.put("total_send", total_send);
	result.put("total_recv", total_recv);
	result.put("total_acked", total_acked);
	result.put("total_process", total_process);
	result.put("total_failed", total_failed);
	return result;
}
	//	把tasks按照bolt-tasks对应好
	@SuppressWarnings("null")
	private Map<String, List<TaskSummary>> Classify(List<String> boltList,
			List<WorkerSummary> workerSummary) {
		// TODO Auto-generated method stub
		
		Map<String, List<TaskSummary>> classified = new TreeMap<String, List<TaskSummary>>();;
		for(String bolt:boltList){
			List<TaskSummary> tl = new ArrayList<TaskSummary>();
			for(WorkerSummary ws:workerSummary){
//				System.out.println("ws");
				List<TaskSummary> taskSummaries = ws.get_tasks();
				for(TaskSummary ts:taskSummaries){
//					System.out.println("ts");
//					是bolts的task还是spouts的task，只选bolts的
					if(ts.get_component_id().equals(bolt)){
//						System.out.println(ts.get_component_id());
						tl.add(ts);
//						taskSummaries.remove(ts);
					}
				}
			}
			System.out.println("boltName:"+bolt+",total tasks number :"+tl.size());
			classified.put(bolt, tl);
		}
		return classified;
	}
}
