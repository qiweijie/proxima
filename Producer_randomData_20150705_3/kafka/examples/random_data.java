package kafka.examples;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Date;
import java.util.Random;


public class random_data {

	public static void main(String[] args) throws IOException {
		Date date = new Date();
		String start_time = date.toString();
		Long start_second = System.currentTimeMillis();
		String filename ="d:\\output_data.txt";
		File file = new File(filename);

		if(file.exists())
			file.delete();
		FileWriter writer = null;
		 writer = new FileWriter(filename, true);   


		//1.循环生成10000记录 10个商品  1000用户
		//生成一个8位数据，将这个8位数分20%   80%

		//user range
		 int max =2000000;
		 int min =1000000;
		
		 //item range
		 int maxP =20000;
		 int minP =10000;
		 StringBuilder stringBuilder = new StringBuilder();

		 for(int i=1;i<=100000000;i++){
			String cid=getCid();
			String uid=String.valueOf(getLongTailNum(max,min));
			String iid=(cid.equals("Chaoleimai")?"H":"Z")+String.valueOf(getLongTailNum(maxP,minP)); 
		    String method="Visit";
		    
		    Long timestamp =System.currentTimeMillis();
		    if(i>1){
		    	stringBuilder.append("\n");
		    }
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
		    
		    if(i%1000000==0){//每10W条输出一次     
		    	System.out.println("随机生成数据第"+i+"条；");
		    }
		    writer.write(stringBuilder.toString()); 
		    stringBuilder = new StringBuilder();
		  
		}
		  writer.close();  
		 date = new Date();
		 Long end_second = System.currentTimeMillis();
		 double spent_minute = (end_second-start_second)/1000.0/60.0;
		 String done_time= date.toString();
		 System.out.println("output done!start_time="+start_time+";done_time="+done_time+";spent_minute="+spent_minute);
		
		 
	}
	
	
	/**
	 * 随机获得cid
	 * @return
	 */
	public static String getCid(){
		String result = "zhangyue";
		Random random = new Random();
		
		if(random.nextInt(10)>=5){
			result="haoleimai";
		}
//		/Random random = new Random();
//		10000000
		return result;
	}
	
	/**
	 * 随机获得cid
	 * @return
	 */
	public static String getMethod(){
		String result = "";
		
		Random random = new Random();
		int random_num = random.nextInt(10);
		if(random_num > 0 && random.nextInt(10) < 3){
			result = "AddCart";
		}else if(random.nextInt(10) >= 3 && random.nextInt(10) < 6){
			result = "Pay";
		}else{
			result= "Visit";
		}
		
//		/Random random = new Random();
//		10000000
		return result;
	}
	

	/**
	 * 取一个范围的随机数
	 * @param max 最大值
	 * @param min 最小值
	 * @return
	 * 	生成8位例子getRandomRangeNum(20000000,10000000);
	 */
	public static int getRandomRangeNum(int max,int min) {
		Random random = new Random();
		int s = random.nextInt(max) % (max - min + 1) + min;
		return s;
	}
	
	
	/**
	 * //生成28 效应数据 20%的机会生成大数  小数据的机会  80%生成大数  
	 * @param max
	 * @param min
	 * @return
	 */
	public static int getLongTailNum(int max,int min){
		int result=0;
		Random random = new Random();
		int s = random.nextInt(10)+1;
		//20%的机会生成
		if(s>2){
			max=(int)(min+min*0.2);
			result=getRandomRangeNum(max,min);
		}else{
			min=(int)(min+min*0.2);
			result=getRandomRangeNum(max,min);
		}
		return result;
	}
	 
}

/*
验证
SELECT cid,COUNT(1) FROM random_data GROUP BY cid; -- cid不平均，zhangyue比好乐买haoleimai 10%
SELECT COUNT(1) FROM random_data WHERE SUBSTR(iid,2,LENGTH(iid))>(10000+10000*0.2) ; -- 202538 商品id 编号在12000-20000之间有20%数据量
SELECT COUNT(1) FROM (SELECT DISTINCT SUBSTR(iid,2,LENGTH(iid)) iid FROM random_data WHERE SUBSTR(iid,2,LENGTH(iid))>(10000+10000*0.2))a -- 7999 ; 
SELECT COUNT(1) FROM  random_data WHERE SUBSTR(iid,2,LENGTH(iid))<=(10000+10000*0.2); -- 797462 商品id 编号在10000-12000之间有80%数据量
SELECT COUNT(1) FROM (SELECT DISTINCT SUBSTR(iid,2,LENGTH(iid)) iid  FROM random_data WHERE SUBSTR(iid,2,LENGTH(iid))<=(10000+10000*0.2)) a; -- 2001

-- 注意设置的uid编号范围不能大于数据记录，否则得到数据分布不同
SELECT COUNT(1) FROM random_data WHERE uid>(100000+100000*0.2) ; -- 200578 商品id 编号在12000-20000之间有20%数据量
SELECT COUNT(1) FROM (SELECT DISTINCT uid FROM random_data WHERE uid >(100000+100000*0.2))a --  65065 ; 
SELECT COUNT(1) FROM  random_data WHERE uid<=(100000+100000*0.2); -- 799422 商品id 编号在10000-12000之间有80%数据量
SELECT COUNT(1) FROM (SELECT DISTINCT  uid  FROM random_data WHERE uid<=(100000+100000*0.2)) a; -- 20001

*/
