package kafka.examples;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class ETL_top_log {
	private static void delFile(String filename){
		File file = new File(filename);
		if(file.exists()){
			file.delete();
		}
	}
	public static void main(String[] args) throws ParseException{
	
		
		String ip = "172.18.1.63";
		
		File file = new File("d:/filter_"+ip+".txt");//数据源文件
		String out_file_total = "d:/top_total"+ip+".log";//total
		String out_file_storm = "d:/top_storm"+ip+".log";//storm thread		
		delFile(out_file_total);
		delFile(out_file_storm);
		
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(file));
            String tempString = null;
            int line = 1;
            // 一次读入一行，直到读入null为文件结束
            String time="";
            String load_average1="";
            String load_average2="";
            String load_average3="";
            int start = 0;
       	    int end = 0;
            String tasks_total="";
            String tasks_running="";
            String tasks_sleeping="";
            String tasks_stopped="";
            String tasks_zombie="";
       	    
       	    String mem_total="";
            String mem_used="";
            String mem_free="";
            String mem_buffers="";
            
            String swap_total="";
            String swap_used="";
            String swap_free="";
            String swap_cached="";
            String date_str ="2015-06-15";

    		String time1= "";
    		Date date=null;
    		Date nextDate=null;
//    		System.out.println(convert2long(time1,"yyyy-MM-dd HH:mm:ss"));
//			date.setTime(date.getTime() + 15*60*1000);
    		String date_format_str = "yyyy-MM-dd HH:mm:ss";
    		SimpleDateFormat date_format = new SimpleDateFormat(date_format_str);
    		int flag=0;
            while ((tempString = reader.readLine()) != null) {
            		
                // 显示行号
                //System.out.println("line " + line + ": " + tempString);
                //记录时间
                if(tempString.startsWith("top -")){
                	
                	time=tempString.substring("top - ".length(),"top - ".length()+8);
                	if(time.startsWith("00:00")){
                		date_str="2015-06-16";
                	}
               	    time=date_str+" "+time.trim();
               	    date = date_format.parse(time);
                	 if(nextDate==null||nextDate.before(date)){
                		 flag=1;
                		 nextDate = new Date();
                		 nextDate.setTime(date.getTime()+ 15*60*1000);
                		 System.out.println("flag:"+flag+";nextDate:"+date_format.format(nextDate)+";date:"+date_format.format(date));
                	 }else {
                		 flag=0;
                		 System.out.println("flag:"+flag+";nextDate:"+date_format.format(nextDate)+";date:"+date_format.format(date));
                		 continue;
                	 }
                	 
//                	 System.out.println(format.format(date).toString());
//                	 if(1==1)
//                		 return ;
                	 int i = tempString.indexOf("load average: ");
                	  start = i+"load average: ".length();
                	  end = i+"load average: ".length()+4;
                      load_average1=tempString.substring(start,end);
                      start = end+" ,".length();
                      end = start+4;
                      load_average2=tempString.substring(start,end);
                      start = end+" ,".length();
                      end = start+4;
                      load_average3=tempString.substring(start,end);
                      
                      //System.out.println("time:"+time+";load_average1:"+load_average1+";load_average2:"+load_average2+";load_average3:"+load_average3);
                }
                //Tasks: 383 total,   1 running, 380 sleeping,   2 stopped,   0 zombie

                if(flag==0){
                	continue;
                }else  if(tempString.startsWith("Tasks: ")){
                	start = "Tasks: ".length();
              	  	end = tempString.indexOf("total,");
                	tasks_total=tempString.substring(start,end);
                	start = "total,".length()+end;
              	  	end = tempString.indexOf("running,");
                	tasks_running=tempString.substring(start,end);
                	start = "running,".length()+end;
              	  	end = tempString.indexOf("sleeping,");
                	tasks_sleeping=tempString.substring(start,end);
                	start = "sleeping,".length()+end;
              	  	end = tempString.indexOf("stopped,");
                	tasks_stopped=tempString.substring(start,end);
                	start = "stopped,".length()+end;
              	  	end = tempString.indexOf("zombie");
                	tasks_zombie=tempString.substring(start,end);
//                	System.out.println("tasks_total:"+tasks_total+";tasks_running:"+tasks_running+";tasks_sleeping:"+tasks_sleeping+";tasks_stopped:"+tasks_stopped+"tasks_zombie:"+tasks_zombie);
                }
//                Mem:  65961552k total, 60479300k used,  5482252k free,   309852k buffers
            
                else if(tempString.startsWith("Mem:")){
                	start = "Mem: ".length();
              	  	end = tempString.indexOf("total,");
              	  mem_total=tempString.substring(start,end);
                	start = "total,".length()+end;
              	  	end = tempString.indexOf("used,");
              	  mem_used=tempString.substring(start,end);
                	start = "used,".length()+end;
              	  	end = tempString.indexOf("free,");
              	  mem_free=tempString.substring(start,end);
                	start = "free,".length()+end;
              	  	end = tempString.indexOf("buffers");
              	  mem_buffers=tempString.substring(start,end);
//                	System.out.println("mem_total:"+mem_total+";mem_used:"+mem_used+";mem_free:"+mem_free+";mem_buffers:"+mem_buffers);
                
                }
//                Swap: 98301724k total,  1859236k used, 96442488k free, 13322900k cached

                else if(tempString.startsWith("Swap:")){
                	start = "Swap: ".length();
              	  	end = tempString.indexOf("total,");
              	  swap_total=tempString.substring(start,end);
                	start = "total,".length()+end;
              	  	end = tempString.indexOf("used,");
              	  swap_used=tempString.substring(start,end);
                	start = "used,".length()+end;
              	  	end = tempString.indexOf("free,");
              	  swap_free=tempString.substring(start,end);
                	start = "free,".length()+end;
              	  	end = tempString.indexOf("cached");
              	  swap_cached=tempString.substring(start,end);
//                	System.out.println("swap_total:"+swap_total+";swap_used:"+swap_used+";swap_free:"+swap_free+";swap_cached:"+swap_cached);
                	String LF = "";
                	if(line>1)
                		LF= "\n";
                	method1(out_file_total,LF+time+";"+load_average1+";"+load_average2+";"+load_average3+";"+tasks_total+";"+tasks_running+";"+tasks_sleeping+";"+tasks_stopped+";"+tasks_zombie+";"+mem_total+";"+mem_used+";"+mem_free+";"+mem_buffers+";"+swap_total+";"+swap_used+";"+swap_free+";"+swap_cached);
                }
                
                else if(tempString.indexOf("storm")>0){
//                	8261 storm     23   2 16.2g  71m 4412 S  2.0  0.1 174:28.30 java
                	String[] storm_str = replaceBlank(tempString).split(" ");
                	DecimalFormat    df   = new DecimalFormat("######0.000"); 
                	
                	 String pid=storm_str[0] ;
                	 String user=storm_str[1] ;
                	 String pr=storm_str[2] ;
                	 String ni=storm_str[3] ;
                	 String virt=storm_str[4] ;//G
                	 double virt_value=convert_storage(virt,"g");
                	 virt=df.format(virt_value); 
                	 String res=storm_str[5] ;//G
                	 double res_value=convert_storage(res,"g");
                	 res=df.format(res_value); 
                	 String shr=storm_str[6] ;//m
                	 double shr_value=convert_storage(shr,"m");
                	 shr=df.format(shr_value);
                	 String s=storm_str[7] ;
                	 String per_cpu=storm_str[8] ;
                	 String per_mem=storm_str[9] ;
                	 String time_plus=storm_str[10] ;
                	 String command=storm_str[11] ;
//                	 System.out.println("virt_value:"+virt+";res_value:"+res+";shr_value:"+shr);
                	
                	String LF = "\n";
//                	System.out.println(pid);
                	method1(out_file_storm,time+";"+pid+";"+user+";"+pr+";"+ni+";"+virt+";"+ res +";"+shr+";"+ s+";"+ per_cpu+";"+ per_mem+";"+ time_plus+";"+command+LF);
                
                
                }
                
                
                
                
                line++;
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                }
            }
        }
		
	}
	
	private static double convert_storage(String virt, String unit) {
		if(virt.indexOf(unit)>0)
			return Double.parseDouble(virt.replace(unit, ""));
		else if(virt.indexOf("m")>0&&unit.equals("g")){//m=>g
			return Double.parseDouble(virt.replace("m", ""))/1024.00;
		}
		else if(virt.indexOf("g")>0&&unit.equals("m")){//g=>m
			return Double.parseDouble(virt.replace("g", ""))*1024.00;
		}
		else if(virt.indexOf("m")<0&&virt.indexOf("g")<0&&unit.equals("g")){//k=>g
			return Double.parseDouble(virt)/1024.00/1024.00;
		}else if(virt.indexOf("m")>0&&virt.indexOf("g")<0&&unit.equals("m")){//k=>m
			return Double.parseDouble(virt)/1024.00;
		}
		
		return 0.00;
	}

	//递归去掉字符串中的空格
	private static String replaceBlank(String str){
		if(str.indexOf(" ")==0)
			str=str.substring(1);
		
		if(str.indexOf("  ")>0){
			str = str.replace("  ", " ");
			str = replaceBlank(str);
		}
		return str;
	}

	/**
     * 以行为单位读取文件，常用于读面向行的格式化文件
     */
    public static void readFileByLines(String fileName) {
    	File file = new File(fileName);
        BufferedReader reader = null;
        try {
            System.out.println("以行为单位读取文件内容，一次读一整行：");
            reader = new BufferedReader(new FileReader(file));
            String tempString = null;
            int line = 1;
            // 一次读入一行，直到读入null为文件结束
            while ((tempString = reader.readLine()) != null) {
                // 显示行号
                System.out.println("line " + line + ": " + tempString);
                line++;
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                }
            }
        }
    }
    public static void method1(String file, String conent) {     
        BufferedWriter out = null;     
        try {     
            out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file, true)));     
            out.write(conent);     
        } catch (Exception e) {     
            e.printStackTrace();     
        } finally {     
            try {     
                if(out != null){  
                    out.close();     
                }  
            } catch (IOException e) {     
                e.printStackTrace();     
            }     
        }     
    }  
    /**
     * 将日期格式的字符串转换为长整型
     * 
     * @param date
     * @param format
     * @return
     */
    public static long convert2long(String date, String format) {
     try {
      
       SimpleDateFormat sf = new SimpleDateFormat(format);
       return sf.parse(date).getTime();
     } catch (ParseException e) {
      e.printStackTrace();
     }
     return 0l;
    }

}
