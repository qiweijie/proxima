package com.baifendian.tools.monitor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.thrift7.TException;
import org.apache.thrift7.protocol.TBinaryProtocol;
import org.apache.thrift7.transport.TFramedTransport;
import org.apache.thrift7.transport.TSocket;
import org.apache.thrift7.transport.TTransportException;
import org.json.simple.JSONObject;

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
			try {
//			从stormTopology里面获取bolt信息，需要先从topoInfo里面获取id
				TopologyInfo topologyInfo = client.getTopologyInfoByName(this.topoName);
				StormTopology stormTopology = client.getTopology(topologyInfo.get_id());
//				获取bolt
				Map<String,Bolt> bolts = stormTopology.get_bolts();
				List<String> boltList = new ArrayList<String>();
				for(String key:bolts.keySet()){
					if(!key.startsWith("__")){
						System.out.println(key);
						boltList.add(key);
					}
				}
//				获取task信息
				List<WorkerSummary> workerSummary = topologyInfo.get_workers();
//				把tasks按照bolt-tasks对应好
				Map<String, List<TaskSummary>> classifiedData = Classify(boltList,workerSummary);
//				对每个bolt的tasks进行处理处理
				Compute(classifiedData);
			} catch (NotAliveException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (TException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} catch (TTransportException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		super.run();
	}
private void Compute(Map<String, List<TaskSummary>> classifiedData) {
		// TODO Auto-generated method stub
		for(String bolt:classifiedData.keySet()){
			List<TaskSummary> temp = classifiedData.get(bolt);
			for(TaskSummary ts:temp){
				Map<String,String> result = ComputeTask(ts);
				WriteToDatabase( result);
			}
		}
}
private void WriteToDatabase(Map<String, String> result) {
	// TODO Auto-generated method stub
	System.out.println(JSONObject.toJSONString(result));
}
// 计算每一个task，返回一个字典
@SuppressWarnings("null")
private Map<String,String> ComputeTask(TaskSummary ts) {
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
	Map<String,String> result=null;
	result.put("total_emitted", total_emitted.toString());
	result.put("total_send", total_send.toString());
	result.put("total_recv", total_recv.toString());
	result.put("total_acked", total_acked.toString());
	result.put("total_process", total_process.toString());
	result.put("total_failed", total_failed.toString());
	return result;
}
	//	把tasks按照bolt-tasks对应好
	@SuppressWarnings("null")
	private Map<String, List<TaskSummary>> Classify(List<String> boltList,
			List<WorkerSummary> workerSummary) {
		// TODO Auto-generated method stub
		Map<String, List<TaskSummary>> classified = new TreeMap<String, List<TaskSummary>>();;
		for(WorkerSummary ws:workerSummary){
			List<TaskSummary> taskSummaries = ws.get_tasks();
			for(String bolt:boltList){
				List<TaskSummary> tl = new ArrayList<TaskSummary>();
				for(TaskSummary ts:taskSummaries){
//					是bolts的task还是spouts的task，只选bolts的
					if(ts.get_component_id().equals(bolt)){
						tl.add(ts);
						taskSummaries.remove(ts);
					}
				}
//				System.out.println(tl);
				classified.put(bolt, tl);
			}
		}
		return classified;
	}
}
