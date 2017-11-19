///*
// * This file is part of las-vpe-platform.
// *
// * las-vpe-platform is free software: you can redistribute it and/or modify
// * it under the terms of the GNU General Public License as published by
// * the Free Software Foundation, either version 3 of the License, or
// * (at your option) any later version.
// *
// * las-vpe-platform is distributed in the hope that it will be useful,
// * but WITHOUT ANY WARRANTY; without even the implied warranty of
// * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// * GNU General Public License for more details.
// *
// * You should have received a copy of the GNU General Public License
// * along with las-vpe-platform. If not, see <http://www.gnu.org/licenses/>.
// *
// * Created by ken.yu on 17-3-13.
// */
//package backup;
//
//import java.io.BufferedReader;
//import java.io.IOException;
//import java.io.InputStreamReader;
//import java.lang.management.ManagementFactory;
//import java.net.InetAddress;
//import java.net.UnknownHostException;
//import java.util.ArrayList;
//import java.util.EnumSet;
//import java.util.List;
//
//import javax.management.InstanceNotFoundException;
//import javax.management.MalformedObjectNameException;
//import javax.management.ReflectionException;
//
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.yarn.api.records.ApplicationReport;
//import org.apache.hadoop.yarn.api.records.ApplicationResourceUsageReport;
//import org.apache.hadoop.yarn.api.records.NodeReport;
//import org.apache.hadoop.yarn.api.records.NodeState;
//import org.apache.hadoop.yarn.api.records.Resource;
//import org.apache.hadoop.yarn.api.records.YarnApplicationState;
//import org.apache.hadoop.yarn.client.api.YarnClient;
//import org.apache.hadoop.yarn.conf.YarnConfiguration;
//import org.apache.hadoop.yarn.exceptions.YarnException;
//import org.apache.kafka.clients.producer.KafkaProducer;
//import org.apache.kafka.clients.producer.ProducerRecord;
//import org.cripac.isee.util.WebToolUtils;
//import org.cripac.isee.vpe.ctrl.SystemPropertyCenter;
//import org.cripac.isee.vpe.entities.Report;
//import org.cripac.isee.vpe.entities.Report.ApplicationInfos;
//import org.cripac.isee.vpe.util.kafka.KafkaHelper;
//import org.cripac.isee.vpe.util.logging.Logger;
//
//import com.google.gson.Gson;
//import com.sun.management.OperatingSystemMXBean;
//
//public class MonitorThread extends Thread {
//
//    public static final String REPORT_TOPIC = "monitor-desc-";
//    
//    private Logger logger;
//    private KafkaProducer<String, String> reportProducer;
//    private Runtime runtime = Runtime.getRuntime();
//    private OperatingSystemMXBean osBean = ManagementFactory.getPlatformMXBean(OperatingSystemMXBean.class);
//
//
//    /**
//     * native 方法
//     * LANG
//     */
//    private native int getInfoCount(int index);
//    
//    private native int getPid(int index,int num);
//    
//    private native long getUsedGpuMemory(int index,int num);
//    
//    
//    private native int initNVML();
//
//    //gpu个数
//    private native int getDeviceCount();
//    
//    private native int getFanSpeed(int index);
//
//    //GPU使用率
//    private native int getUtilizationRate(int index);
//
//    private native long getFreeMemory(int index);
//
//    private native long getTotalMemory(int index);
//
//    private native long getUsedMemory(int index);
//
//    private native int getTemperature(int index);
//
//    private native int getSlowDownTemperatureThreshold(int index);
//
//    private native int getShutdownTemperatureThreshold(int index);
//
//    private native int getPowerLimit(int index);
//
//    private native int getPowerUsage(int index);
//
//    
//    public MonitorThread(Logger logger, SystemPropertyCenter propCenter)
//            throws MalformedObjectNameException, ReflectionException, InstanceNotFoundException {
//        this.logger = logger;
//        this.reportProducer = new KafkaProducer<>(propCenter.getKafkaProducerProp(true));
//
//        KafkaHelper.createTopic(propCenter.zkConn, propCenter.zkSessionTimeoutMs, propCenter.zkConnectionTimeoutMS,
//                REPORT_TOPIC,
//                propCenter.kafkaNumPartitions, propCenter.kafkaReplFactor);
//
//        logger.info("Running with " + osBean.getAvailableProcessors() + " " + osBean.getArch() + " processors");
//        
//    }
//    
//    
//    public MonitorThread(){
//    	
//    }
//
//	@Override
//    public void run() {
//    	Report report =getReport();
//        //noinspection InfiniteLoopStatement
//        int count=0;
//        while (true) {
//        	getDevInfo(report,count);
//        	try {
////				appReport(report);
//				getClusterReport(report);
//			} catch (YarnException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			} catch (IOException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
//        	getAppInfoByNode(report);
//        	
//        	logger.info(new Gson().toJson(report));
//            this.reportProducer.send(new ProducerRecord<>(REPORT_TOPIC, getNodeName(), new Gson().toJson(report)));
//            
//            clearList(report);
//            try {
//                sleep(10000);
//            } catch (InterruptedException ignored) {
//            }
//            count++;
////            if (count>1) {
////            	report.processNumList.clear();
////            }
//        }
//    }
//
//	public void clearList(Report report){
//		for (int i = 0; i < report.deviceCount; ++i) {
//        	Report.DevInfo info = report.devInfos[i];
//        	List<Integer> processNumList=info.processNumList;
//        	if (processNumList!=null) {
//				
//        		processNumList.clear();
//			}
//        }
//		List<Integer> processNumAllList=report.processNumAllList;
//		List<Report.DevInfo.ProcessesDevInfo> processAllList=report.processAllList;
//		if (processNumAllList!=null) {
//			
//			processNumAllList.clear();
//		}
//		if (processAllList!=null) {
//			
//			processAllList.clear();
//		}
//	}
//	
//	/**
//	 * 加载动态链接库
//	 */
//    static {
//        org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(MonitorThread.class);
//        try {
//            logger.info("Loading native libraries for MonitorThread from "
//                    + System.getProperty("java.library.path"));
//            System.loadLibrary("CudaMonitor4j");
////            System.load("/home/jun.li/monitor.test/src/native/CudaMonitor4j/Release/libCudaMonitor4j.so");
//            logger.info("Native libraries for BasicTracker successfully loaded!");
//        } catch (Throwable t) {
//            logger.error("Failed to load native library for MonitorThread", t);
//            throw t;
//        }
//    }
//    
//    public Report getReport(){
//    	Report report = new Report();
//    	
//        report.nodeName = getNodeName();
//        report.ip=getIp();
//        
//        logger.info("hostname："+report.nodeName+",ip:"+report.ip);
//
//        report.cpuNum=getCpuNum();
//        report.cpuCore=getCpuCore();
//        
//        int nvmlInitRet = initNVML();
//        if (nvmlInitRet == 0) {
//        	report.deviceCount = getDeviceCount();
//            logger.info("Running with " + report.deviceCount + " GPUs.");
//        } else {
//        	report.deviceCount = 0;
//            logger.info("Cannot initialize NVML: " + nvmlInitRet);
//        }
//        
//        report.devInfos = new Report.DevInfo[report.deviceCount];
//        report.devNumList=new ArrayList<>();
//        
//        
//        for (int i = 0; i < report.deviceCount; ++i) {
//            report.devInfos[i] = new Report.DevInfo();
//            report.devNumList.add(i);
//        }
//
//        logger.debug("Starting monitoring gpu!");
//        return report;
//    }
//    
//    
//    public void getDevInfo(Report report,int count){
//    	report.cpuVirtualNum=getCpuVirtualNum();
//    	   											// 剩余内存
//    	report.usedMem = (runtime.totalMemory() - runtime.freeMemory()) / (1024 * 1024);
//        report.jvmMaxMem = runtime.maxMemory() / (1024 * 1024);
//        report.jvmTotalMem = runtime.totalMemory() / (1024 * 1024);
//        report.physicTotalMem = osBean.getTotalPhysicalMemorySize() / (1024 * 1024);
//        
//        logger.info("Memory consumption: "
//                + report.usedMem + "/"
//                + report.jvmMaxMem + "/"
//                + report.jvmTotalMem + "/"
//                + report.physicTotalMem + "M");
//
//        report.procCpuLoad = (int) (osBean.getProcessCpuLoad() * 100);
//        report.sysCpuLoad = (int) (osBean.getSystemCpuLoad() * 100);
//        logger.info("CPU load: " + report.procCpuLoad + "/" + report.sysCpuLoad + "%");
//
//        StringBuilder stringBuilder = new StringBuilder("GPU Usage:");
//        report.processNumAllList=new ArrayList<>();
//        report.processAllList=new ArrayList<>();
//        for (int i = 0; i < report.deviceCount; ++i) {
//            Report.DevInfo info = report.devInfos[i];
//            info.index=i;
//            info.fanSpeed = getFanSpeed(i);
//            info.utilRate = getUtilizationRate(i);
//            info.usedMem = getUsedMemory(i);
//            info.totalMem = getTotalMemory(i);
//            info.temp = getTemperature(i);
//            info.slowDownTemp = getSlowDownTemperatureThreshold(i);
//            info.shutdownTemp = getShutdownTemperatureThreshold(i);
//            info.powerUsage = getPowerUsage(i);
//            info.powerLimit = getPowerLimit(i);
//            info.infoCount=getInfoCount(i);
//            info.processNumList=new ArrayList<>();
//            info.processesDevInfos=new Report.DevInfo.ProcessesDevInfo[info.infoCount];
////            if (count==0) {
//				
//            	for (int j = 0; j < info.infoCount; j++) {
//            		info.processesDevInfos[j]=new Report.DevInfo.ProcessesDevInfo();
//            		info.processNumList.add(i);
//            	}
//            	for (int j = 0; j < info.infoCount; j++) {
//					Report.DevInfo.ProcessesDevInfo processesDevInfo = info.processesDevInfos[j];
//					processesDevInfo.usedGpuMemory = getUsedGpuMemory(i,j)/ (1024 * 1024);
//					processesDevInfo.pid = getPid(i,j);
//					processesDevInfo.index=i;
//					report.processAllList.add(processesDevInfo);
//            	}
////			}
//            	report.processNumAllList.addAll(info.processNumList);	
//            stringBuilder.append("\n|Index\t|Fan\t|Util\t|Mem(MB)\t|Temp(C)\t|Pow\t|infoCount");
//            stringBuilder.append("\n|").append(info.index)
//                    .append("\t|")
//                    .append(info.fanSpeed)
//                    .append("\t|")
//                    .append(String.format("%3d", info.utilRate)).append('%')
//                    .append("\t|")
//                    .append(String.format("%5d", info.usedMem / (1024 * 1024)))
//                    .append("/").append(String.format("%5d", info.totalMem / (1024 * 1024)))
//                    .append("\t|")
//                    .append(info.temp).append("/").append(info.slowDownTemp).append("/").append(info.shutdownTemp)
//                    .append("\t|")
//                    .append(info.powerUsage).append("/").append(info.powerLimit)
//            		.append("\t|")
//            		.append(info.infoCount);
//        }
//        
//        logger.info(stringBuilder.toString());
//        
//        
////        return report;
//    }
//    
//    public String getNodeName(){
//    	String nodeName1;
//        try {
//            nodeName1 = InetAddress.getLocalHost().getHostName();
//        } catch (UnknownHostException e) {
//            nodeName1 = "Unknown host";
//        }
//        return nodeName1;
//    }
//    
//    public String getIp(){
//    	String ipString;
//        try {
//        	ipString=WebToolUtils.getLocalIP();
//		} catch (Exception e) {
//			// TODO: handle exception
//			ipString="Unknown ip";
//		}
//        return ipString;
//    }
//    
//	public int getCpuNum() {
//		final String[] command = {"/bin/sh","-c","cat /proc/cpuinfo | grep 'physical id'| sort | uniq | wc -l"};
//		String message = getCommandResult(command);
//		String result = "";
//		result = message.split(" ")[0].trim();
//		System.out.println("cpu的数量是："+result);
//		return Integer.parseInt(result);
//	}
//    
//    
//    
//    public int getCpuCore() {
//    	final String[] command={"/bin/sh","-c","/usr/bin/cat /proc/cpuinfo | grep 'cpu cores' | uniq"};
//    	String message =getCommandResult(command);
//    	String result="";
//    	if (message.contains(":")) {
//    		result=message.split(":")[1].trim();
//		}else if(message.contains("：")){
//			result=message.split("：")[1].trim();
//		}
//    	return Integer.parseInt(result);
//    }
//    
//    public int getCpuVirtualNum() {
//    	final String[] command={"/bin/sh","-c","/usr/bin/cat /proc/cpuinfo | grep 'processor' | wc -l"};
//    	String message =getCommandResult(command);
//    	String result = "";
//		result = message.split(" ")[0].trim();
//		return Integer.parseInt(result);
//    }
//    public String getCommandResult(String[] command) {
//    	Process process = null;
//		try {
//			process = runtime.exec(command);
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//    	BufferedReader bufReader = new BufferedReader(new InputStreamReader(process.getInputStream()));
//    	StringBuffer sb = new StringBuffer();
//    	String line = "";  
//    	try {
//			while((line = bufReader.readLine()) != null) {
////				System.out.println("测试：");
////				System.out.println("getCommandResult:"+line);
//			    sb.append(line).append("\n");
//			}
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//    	return sb.toString();
//    }
//    
//    /**
//	 * 得到yarn上的application的cpu和内存
//	 * LANG
//	 * @throws YarnException
//	 * @throws IOException
//	 */
//	public void appReport(Report report,YarnClient yarnClient) throws YarnException, IOException {
//// 		ApplicationResourceUsageReport applicationResourceUsageReport=new ApplicationReportPBImpl().getApplicationResourceUsageReport();
//		
//		List<ApplicationReport> list = yarnClient.getApplications(EnumSet.of(YarnApplicationState.RUNNING));
//		if (list != null) {
//			if (list.size() > 0) {
//				
//				report.applicationInfos = new Report.ApplicationInfos[list.size()];
//				for (int i = 0; i < list.size(); i++) {
//					report.applicationInfos[i]=new Report.ApplicationInfos();
//				}
//				
//				for (int i = 0; i < list.size(); i++) {
//					ApplicationReport applicationReport=list.get(i);
//					ApplicationResourceUsageReport applicationResourceUsageReport = applicationReport.getApplicationResourceUsageReport();
//					Report.ApplicationInfos applicationInfo = report.applicationInfos[i];
//					
//					//MB
////					long memory = applicationResourceUsageReport.getMemorySeconds();
////					long vcore = applicationResourceUsageReport.getVcoreSeconds();
////					long preemptedMemory=applicationResourceUsageReport.getPreemptedMemorySeconds();
////					long preemptedVcore=applicationResourceUsageReport.getPreemptedVcoreSeconds();
//					//需要的
//					Resource neededResource = applicationResourceUsageReport.getNeededResources();
//					//使用的
//					Resource usedResource = applicationResourceUsageReport.getUsedResources();
//					//保留的
//					Resource reservedResource = applicationResourceUsageReport.getReservedResources();
//					
//					applicationInfo.applicationId=applicationReport.getApplicationId()+"";
//					applicationInfo.neededResourceMemory=neededResource.getMemory();
//					applicationInfo.neededResourceVcore=neededResource.getVirtualCores();
//					applicationInfo.usedResourceMemory=usedResource.getMemory();
//					applicationInfo.usedResourceVcore=usedResource.getVirtualCores();
//					applicationInfo.reservedResourceMemory=reservedResource.getMemory();
//					applicationInfo.reservedResourceVcore=reservedResource.getVirtualCores();
//					
//					System.out.println("ApplicationId:"+applicationInfo.applicationId
//							+",neededResourceMemory:"+applicationInfo.neededResourceMemory+",neededResourceVcore:"+applicationInfo.neededResourceVcore
//							+",usedResourceMemory:"+applicationInfo.usedResourceMemory+",usedResourceVcore:"+applicationInfo.usedResourceVcore
//							+",reservedResourceMemory:"+applicationInfo.reservedResourceMemory+",reservedResourceVcore:"+applicationInfo.reservedResourceVcore);
//				}
//			}
//		}
//	}
//	
//	public void getClusterReport(Report report)throws YarnException, IOException{
//		YarnClient yarnClient = getYarnClient();
//		appReport(report,yarnClient);
//	}
//	
//	/**
//	 * 将该方法放在spark或者hadoop的分布式程序中，可以得到该程序在每台节点上的pid
//	 */
//	public int getPID() {
//	    String processName = java.lang.management.ManagementFactory.getRuntimeMXBean().getName();
//	    if (processName != null && processName.length() > 0) {
//	        try {
//	            return Integer.parseInt(processName.split("@")[0]);
//	        }
//	        catch (Exception e) {
//	            return 0;
//	        }
//	    }
//
//	    return 0;
//	}
//	
//	public void getAppInfoByNode(Report report){
//		ApplicationInfos[] applicationInfos=report.applicationInfos;
//		for (int i = 0; i < applicationInfos.length; i++) {
//			Report.ApplicationInfos applicationInfo = report.applicationInfos[i];
//			applicationInfo.pid=getPID();
//			List<Report.DevInfo.ProcessesDevInfo> processAllList=report.processAllList;
//			if (processAllList!=null) {
//				if (processAllList.size()>0) {
//					
//					for (int j = 0; j < processAllList.size(); j++) {
//						Report.DevInfo.ProcessesDevInfo processesDevInfo=processAllList.get(j);
//						if (processesDevInfo.pid==applicationInfo.pid) {
//							applicationInfo.index=processesDevInfo.index;
//							applicationInfo.usedGpuMemory=processesDevInfo.usedGpuMemory;
//						}
//					}
//				}
//			}
//			applicationInfo.ip=report.ip;
//			applicationInfo.nodeName=report.nodeName;
//		}
//	}
//	/**
//	 * 得到yarn
//	 * LANG
//	 * @return
//	 */
//	public YarnClient getYarnClient(){
//		YarnConfiguration conf = new YarnConfiguration();
////		conf.set("fs.defaultFS", "hdfs://rtask-nod8:8020");
//		conf.set("yarn.resourcemanager.scheduler.address", "rtask-nod8:8030");
//		String hadoopHome = System.getenv("HADOOP_HOME");
//		conf.addResource(new Path(hadoopHome + "/etc/hadoop/core-site.xml"));
//		conf.addResource(new Path(hadoopHome + "/etc/hadoop/yarn-site.xml"));
//		YarnClient yarnClient = YarnClient.createYarnClient();
//		yarnClient.init(conf);
//		yarnClient.start();
//		return yarnClient;
//	}
//	
//	public void getNodes(Report report,YarnClient yarnClient) throws YarnException, IOException{
//		List<NodeReport> nodeList=yarnClient.getNodeReports(NodeState.RUNNING);
//		for (int i = 0; i < nodeList.size(); i++) {
//			NodeReport nodeReport=nodeList.get(i);
//			nodeReport.getNodeId();
//			
//		}
//	}
//}
