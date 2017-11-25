
package org.cripac.isee.vpe.ctrl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.management.InstanceNotFoundException;
import javax.management.MalformedObjectNameException;
import javax.management.ReflectionException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptReport;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationResourceUsageReport;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerReport;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.cripac.isee.util.WebToolUtils;
import org.cripac.isee.vpe.entities.Report;
import org.cripac.isee.vpe.entities.Report.ClusterInfo;
import org.cripac.isee.vpe.entities.Report.ClusterInfo.ApplicationInfos;
import org.cripac.isee.vpe.entities.Report.ClusterInfo.ApplicationInfos.ContarinerInfos;
import org.cripac.isee.vpe.entities.Report.ClusterInfo.ApplicationInfos.EachAppNode;
import org.cripac.isee.vpe.entities.Report.ClusterInfo.Nodes;
import org.cripac.isee.vpe.entities.Report.ServerInfo;
import org.cripac.isee.vpe.entities.Report.ServerInfo.DevInfo;
import org.cripac.isee.vpe.entities.Report.ServerInfo.DevInfo.ProcessesDevInfo;
import org.cripac.isee.vpe.util.kafka.KafkaHelper;
import org.cripac.isee.vpe.util.logging.Logger;

import com.google.gson.Gson;
import com.sun.management.OperatingSystemMXBean;


public class MonitorThread extends Thread {

    public static final String REPORT_TOPIC = "monitor-desc-";
    
    private Logger logger;
    private KafkaProducer<String, String> reportProducer;
    private Runtime runtime = Runtime.getRuntime();
    private OperatingSystemMXBean osBean = ManagementFactory.getPlatformMXBean(OperatingSystemMXBean.class);


    /**
     * native 方法
     * LANG
     */
    private native int getInfoCount(int index);
    
    private native int getPid(int index,int num);
    
    private native long getUsedGpuMemory(int index,int num);
    
    
    private native int initNVML();

    //gpu个数
    private native int getDeviceCount();
    
    private native int getFanSpeed(int index);

    //GPU使用率
    private native int getUtilizationRate(int index);

    private native long getFreeMemory(int index);

    private native long getTotalMemory(int index);

    private native long getUsedMemory(int index);

    private native int getTemperature(int index);

    private native int getSlowDownTemperatureThreshold(int index);

    private native int getShutdownTemperatureThreshold(int index);

    private native int getPowerLimit(int index);

    private native int getPowerUsage(int index);

    
    public MonitorThread(Logger logger, SystemPropertyCenter propCenter)
            throws MalformedObjectNameException, ReflectionException, InstanceNotFoundException {
        this.logger = logger;
        this.reportProducer = new KafkaProducer<>(propCenter.getKafkaProducerProp(true));

        KafkaHelper.createTopic(propCenter.zkConn, propCenter.zkSessionTimeoutMs, propCenter.zkConnectionTimeoutMS,
                REPORT_TOPIC+getServerName(),
                propCenter.kafkaNumPartitions, propCenter.kafkaReplFactor);

        logger.info("Running with " + osBean.getAvailableProcessors() + " " + osBean.getArch() + " processors");
        
    }
    
    
    public MonitorThread(){
    	
    }
    
	/**
	 * 加载动态链接库
	 */
    static {
        org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(MonitorThread.class);
        try {
            logger.info("Loading native libraries for MonitorThread from "
                    + System.getProperty("java.library.path"));
            System.loadLibrary("CudaMonitor4j");
//            System.load("/home/jun.li/monitor.test/src/native/CudaMonitor4j/Release/libCudaMonitor4j.so");
            logger.info("Native libraries for BasicTracker successfully loaded!");
        } catch (Throwable t) {
            logger.error("Failed to load native library for MonitorThread", t);
            throw t;
        }
    }

	@Override
    public void run() {
    	Report report =initReport();
    	ServerInfo serverInfo=getServerInfoReport(report);
        //noinspection InfiniteLoopStatement
        int count=0;
        while (true) {
        	getDevInfo(report,serverInfo,count);
        	try {
//				appReport(report);
				getClusterReport(report);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} 
        	getAppInfoByNode(report,serverInfo);
        	
        	logger.info(new Gson().toJson(report));
            this.reportProducer.send(new ProducerRecord<>(REPORT_TOPIC+getServerName(), getServerName(), new Gson().toJson(report)));
            
            clearList(report,serverInfo);
            try {
                sleep(10000);
            } catch (InterruptedException ignored) {
            }
            count++;
//            if (count>1) {
//            	report.processNumList.clear();
//            }
        }
    }

	public void clearList(Report report,ServerInfo serverInfo){
		for (int i = 0; i < serverInfo.deviceCount; ++i) {
        	DevInfo info = serverInfo.devInfosList.get(i);
        	List<Integer> processNumList=info.processNumList;
        	if (processNumList!=null) {
				
        		processNumList.clear();
			}
        }
		List<Integer> processNumAllList=serverInfo.processNumAllList;
		List<ProcessesDevInfo> processAllList=serverInfo.processAllList;
		if (processNumAllList!=null) {
			
			processNumAllList.clear();
		}
		if (processAllList!=null) {
			
			processAllList.clear();
		}
	}
	

    
    public Report initReport(){
    	Report report = new Report();
    	
    	report.serverInfosMap=new HashMap<>();
    	String nodeName=getServerName();
    	ServerInfo serverInfo=new ServerInfo();
    	//put之后才有值，同时可以再put之后再给其中的对象赋值，是可以拿到的
    	report.serverInfosMap.put(nodeName, serverInfo);
    	report.clusterInfo=new ClusterInfo();
    	
    	return report;
    }
    
    public ServerInfo getServerInfoReport(Report report){
    	
    	Map <String,ServerInfo> serverInfosMap=report.serverInfosMap;
    	ServerInfo serverInfo=null;
    	for (Entry<String, ServerInfo> entry : serverInfosMap.entrySet()) {
			serverInfo=entry.getValue();
		}
    	serverInfo.nodeName = getServerName();
    	serverInfo.ip=getIp();
        
        logger.info("hostname："+serverInfo.nodeName+",ip:"+serverInfo.ip);

        serverInfo.cpuNum=getCpuNum();
        serverInfo.cpuCore=getCpuCore();
        
        int nvmlInitRet = initNVML();
        if (nvmlInitRet == 0) {
        	serverInfo.deviceCount = getDeviceCount();
            logger.info("Running with " + serverInfo.deviceCount + " GPUs.");
        } else {
        	serverInfo.deviceCount = 0;
            logger.info("Cannot initialize NVML: " + nvmlInitRet);
        }
        
        serverInfo.devInfosList = new ArrayList<>();
        serverInfo.devNumList=new ArrayList<>();
        
        
        for (int i = 0; i < serverInfo.deviceCount; ++i) {
        	serverInfo.devInfosList.add(new DevInfo());
            serverInfo.devNumList.add(i);
        }

        logger.debug("Starting monitoring gpu!");
        return serverInfo;
    }
    
    
    public void getDevInfo(Report report,ServerInfo serverInfo,int count){
    	
    	serverInfo.cpuVirtualNum=getCpuVirtualNum();
    	   											// 剩余内存
    	serverInfo.usedMem = (runtime.totalMemory() - runtime.freeMemory()) / (1024 * 1024);
    	serverInfo.jvmMaxMem = runtime.maxMemory() / (1024 * 1024);
    	serverInfo.jvmTotalMem = runtime.totalMemory() / (1024 * 1024);
    	serverInfo.physicTotalMem = osBean.getTotalPhysicalMemorySize() / (1024 * 1024);
        
        logger.info("Memory consumption: "
                + serverInfo.usedMem + "/"
                + serverInfo.jvmMaxMem + "/"
                + serverInfo.jvmTotalMem + "/"
                + serverInfo.physicTotalMem + "M");

        serverInfo.procCpuLoad = (int) (osBean.getProcessCpuLoad() * 100);
        serverInfo.sysCpuLoad = (int) (osBean.getSystemCpuLoad() * 100);
        logger.info("CPU load: " + serverInfo.procCpuLoad + "/" + serverInfo.sysCpuLoad + "%");

        StringBuilder stringBuilder = new StringBuilder("GPU Usage:");
        serverInfo.processNumAllList=new ArrayList<>();
        serverInfo.processAllList=new ArrayList<>();
        for (int i = 0; i < serverInfo.deviceCount; ++i) {
            DevInfo devInfo = serverInfo.devInfosList.get(i);
            devInfo.index=i;
            devInfo.fanSpeed = getFanSpeed(i);
            devInfo.utilRate = getUtilizationRate(i);
            devInfo.usedMem = getUsedMemory(i);
            devInfo.totalMem = getTotalMemory(i);
            devInfo.temp = getTemperature(i);
            devInfo.slowDownTemp = getSlowDownTemperatureThreshold(i);
            devInfo.shutdownTemp = getShutdownTemperatureThreshold(i);
            devInfo.powerUsage = getPowerUsage(i);
            devInfo.powerLimit = getPowerLimit(i);
            devInfo.infoCount=getInfoCount(i);
            devInfo.processNumList=new ArrayList<>();
            devInfo.processesDevInfosList=new ArrayList<>();
//            if (count==0) {
				
//            	for (int j = 0; j < devInfo.infoCount; j++) {
//            		devInfo.processesDevInfosList.add(new ProcessesDevInfo());
//            		devInfo.processNumList.add(i);
//            	}
            	for (int j = 0; j < devInfo.infoCount; j++) {
            		
					ProcessesDevInfo processesDevInfo = new ProcessesDevInfo();
//							devInfo.processesDevInfosList.get(j);
					processesDevInfo.usedGpuMemory = getUsedGpuMemory(i,j)/ (1024 * 1024);
					processesDevInfo.pid = getPid(i,j);
					processesDevInfo.index=i;
					devInfo.processesDevInfosList.add(processesDevInfo);
					serverInfo.processAllList.add(processesDevInfo);
					devInfo.processNumList.add(i);
            	}
//			}
            	serverInfo.processNumAllList.addAll(devInfo.processNumList);	
            stringBuilder.append("\n|Index\t|Fan\t|Util\t|Mem(MB)\t|Temp(C)\t|Pow\t|infoCount");
            stringBuilder.append("\n|").append(devInfo.index)
                    .append("\t|")
                    .append(devInfo.fanSpeed)
                    .append("\t|")
                    .append(String.format("%3d", devInfo.utilRate)).append('%')
                    .append("\t|")
                    .append(String.format("%5d", devInfo.usedMem / (1024 * 1024)))
                    .append("/").append(String.format("%5d", devInfo.totalMem / (1024 * 1024)))
                    .append("\t|")
                    .append(devInfo.temp).append("/").append(devInfo.slowDownTemp).append("/").append(devInfo.shutdownTemp)
                    .append("\t|")
                    .append(devInfo.powerUsage).append("/").append(devInfo.powerLimit)
            		.append("\t|")
            		.append(devInfo.infoCount);
        }
        
        logger.info(stringBuilder.toString());
        
        
//        return report;
    }
    
    public String getServerName(){
    	String nodeName1;
        try {
            nodeName1 = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            nodeName1 = "Unknown host";
        }
        return nodeName1;
    }
    
    public String getIp(){
    	String ipString;
        try {
        	ipString=WebToolUtils.getLocalIP();
		} catch (Exception e) {
			// TODO: handle exception
			ipString="Unknown ip";
		}
        return ipString;
    }
    
	public int getCpuNum() {
		final String[] command = {"/bin/sh","-c","cat /proc/cpuinfo | grep 'physical id'| sort | uniq | wc -l"};
		String message = getCommandResult(command);
		String result = "";
		result = message.split(" ")[0].trim();
		System.out.println("cpu的数量是："+result);
		return Integer.parseInt(result);
	}
    
    
    
    public int getCpuCore() {
    	final String[] command={"/bin/sh","-c","/usr/bin/cat /proc/cpuinfo | grep 'cpu cores' | uniq"};
    	String message =getCommandResult(command);
    	String result="";
    	if (message.contains(":")) {
    		result=message.split(":")[1].trim();
		}else if(message.contains("：")){
			result=message.split("：")[1].trim();
		}
    	return Integer.parseInt(result);
    }
    
    public int getCpuVirtualNum() {
    	final String[] command={"/bin/sh","-c","/usr/bin/cat /proc/cpuinfo | grep 'processor' | wc -l"};
    	String message =getCommandResult(command);
    	String result = "";
		result = message.split(" ")[0].trim();
		return Integer.parseInt(result);
    }
    public String getCommandResult(String[] command) {
    	Process process = null;
		try {
			process = runtime.exec(command);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	BufferedReader bufReader = new BufferedReader(new InputStreamReader(process.getInputStream()));
    	StringBuffer sb = new StringBuffer();
    	String line = "";  
    	try {
			while((line = bufReader.readLine()) != null) {
//				System.out.println("测试：");
//				System.out.println("getCommandResult:"+line);
			    sb.append(line).append("\n");
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	return sb.toString();
    }
    
    
    public void getClusterReport(Report report)throws YarnException, IOException{
		YarnClient yarnClient = getYarnClient();
		getNodes(report, yarnClient);
		appReport(report,yarnClient);
	}
    
    /**
	 * 得到yarn上的application的cpu和内存
	 * LANG
	 * @throws YarnException
	 * @throws IOException
	 */
	public void appReport(Report report,YarnClient yarnClient) throws YarnException, IOException {
// 		ApplicationResourceUsageReport applicationResourceUsageReport=new ApplicationReportPBImpl().getApplicationResourceUsageReport();
		
		List<ApplicationReport> list = yarnClient.getApplications(EnumSet.of(YarnApplicationState.RUNNING));
		if (list != null) {
			if (list.size() > 0) {
				List<ApplicationInfos> applicationInfosList=new ArrayList<>();
				report.clusterInfo.applicationInfosList=applicationInfosList;
//				report.clusterInfo.applicationInfosList=new ArrayList<>();
//				report.applicationInfos = new Report.ApplicationInfos[list.size()];
//				for (int i = 0; i < list.size(); i++) {
////					report.applicationInfos[i]=new Report.ApplicationInfos();
//					report.clusterInfo.applicationInfosList.add(new ApplicationInfos());
//				}
				
				for (int i = 0; i < list.size(); i++) {
					ApplicationReport applicationReport=list.get(i);
					ApplicationResourceUsageReport applicationResourceUsageReport = applicationReport.getApplicationResourceUsageReport();
					ApplicationInfos applicationInfo = new ApplicationInfos();
//					report.clusterInfo.applicationInfosList.get(i);
					report.clusterInfo.applicationInfosList.add(applicationInfo);
					//MB
//					long memory = applicationResourceUsageReport.getMemorySeconds();
//					long vcore = applicationResourceUsageReport.getVcoreSeconds();
//					long preemptedMemory=applicationResourceUsageReport.getPreemptedMemorySeconds();
//					long preemptedVcore=applicationResourceUsageReport.getPreemptedVcoreSeconds();
					//需要的
					Resource neededResource = applicationResourceUsageReport.getNeededResources();
					//使用的
					Resource usedResource = applicationResourceUsageReport.getUsedResources();
					//保留的
					Resource reservedResource = applicationResourceUsageReport.getReservedResources();
					
					ApplicationId applicationId=applicationReport.getApplicationId();
					applicationInfo.applicationId=applicationId+"";
					applicationInfo.neededResourceMemory=neededResource.getMemory();
					applicationInfo.neededResourceVcore=neededResource.getVirtualCores();
					applicationInfo.usedResourceMemory=usedResource.getMemory();
					applicationInfo.usedResourceVcore=usedResource.getVirtualCores();
					applicationInfo.reservedResourceMemory=reservedResource.getMemory();
					applicationInfo.reservedResourceVcore=reservedResource.getVirtualCores();
					
					System.out.println("ApplicationId:"+applicationInfo.applicationId
							+",neededResourceMemory:"+applicationInfo.neededResourceMemory+",neededResourceVcore:"+applicationInfo.neededResourceVcore
							+",usedResourceMemory:"+applicationInfo.usedResourceMemory+",usedResourceVcore:"+applicationInfo.usedResourceVcore
							+",reservedResourceMemory:"+applicationInfo.reservedResourceMemory+",reservedResourceVcore:"+applicationInfo.reservedResourceVcore);
					
					//container
					getContainers(report,applicationInfo, yarnClient, applicationId);
				}
			}
		}
	}
	
	
	
	
	
	public void getAppInfoByNode(Report report,ServerInfo serverInfo){
		List<ApplicationInfos> applicationInfosList=report.clusterInfo.applicationInfosList;
//		report.clusterInfo.applicationInfosList=applicationInfosList;
		if (applicationInfosList!=null) {
			if (applicationInfosList.size()>0) {
				
				for (int i = 0; i < applicationInfosList.size(); i++) {
					ApplicationInfos applicationInfo = applicationInfosList.get(i);
					applicationInfo.eachAppNodeMap=new HashMap<>();
					EachAppNode eachAppNode=new EachAppNode();
					String nodeName=getServerName();
					applicationInfo.eachAppNodeMap.put(nodeName, eachAppNode);
					eachAppNode.pid=getPID();
					eachAppNode.ip=serverInfo.ip;
					eachAppNode.nodeName=serverInfo.nodeName;
					List<ProcessesDevInfo> processAllList=serverInfo.processAllList;
					if (processAllList!=null) {
						if (processAllList.size()>0) {
							
							for (int j = 0; j < processAllList.size(); j++) {
								ProcessesDevInfo processesDevInfo=processAllList.get(j);
								if (processesDevInfo.pid==eachAppNode.pid) {
									eachAppNode.index=processesDevInfo.index;
									eachAppNode.usedGpuMemory=processesDevInfo.usedGpuMemory;
								}
							}
						}
					}
				}
			}
		}
	}
	
	
	public void getNodes(Report report,YarnClient yarnClient) throws YarnException, IOException{
		List<NodeReport> nodeList=yarnClient.getNodeReports(NodeState.RUNNING);
		List<Nodes> nodeInfosList=new ArrayList<>();
		report.clusterInfo.nodeInfosList=nodeInfosList;
//		for (int i = 0; i < nodeList.size(); i++) {
//			nodeInfosList.add(new Nodes());
//		}
		for (int i = 0; i < nodeList.size(); i++) {
			Nodes nodes=new Nodes();
			nodeInfosList.add(nodes);
			
			NodeReport nodeReport=nodeList.get(i);
			nodes.name=nodeReport.getNodeId().getHost();
			
			Resource capResource=nodeReport.getCapability();
			nodes.capabilityCpu=capResource.getVirtualCores();
			nodes.capabilityMemory=capResource.getMemory();
			
			Resource usedResource=nodeReport.getUsed();
			nodes.usedCpu=usedResource.getVirtualCores();
			nodes.usedMemory=usedResource.getMemory();
			
		}
	}
	
	public void getContainers(Report report, ApplicationInfos applicationInfo, YarnClient yarnClient,
			ApplicationId applicationId) throws YarnException, IOException {
		List<ApplicationAttemptReport> applicationAttemptReportsList = yarnClient.getApplicationAttempts(applicationId);
		List<ContarinerInfos> contarinerInfosList = new ArrayList<>();
		applicationInfo.contarinerInfosList = contarinerInfosList;
		if (applicationAttemptReportsList != null) {
			if (applicationAttemptReportsList.size() > 0) {
				System.out.println("每个application的attempt的大小是：" + applicationAttemptReportsList.size());
				for (int i = 0; i < applicationAttemptReportsList.size(); i++) {
					ApplicationAttemptReport applicationAttemptReport = applicationAttemptReportsList.get(i);
					List<ContainerReport> containerReportsList = yarnClient.getContainers(applicationAttemptReport.getApplicationAttemptId());
					if (containerReportsList != null) {
						if (containerReportsList.size() > 0) {
							for (int j = 0; j < containerReportsList.size(); j++) {
								ContainerReport containerReport = containerReportsList.get(j);
								ContainerId containerId = containerReport.getContainerId();
								ContainerState containerState = containerReport.getContainerState();
								Resource allocatedResource = containerReport.getAllocatedResource();
								String hostName=containerReport.getAssignedNode().getHost();
								String DiagnosticsInfo=containerReport.getDiagnosticsInfo();
								String logurlString=containerReport.getLogUrl();
								String NodeHttpAddress=containerReport.getNodeHttpAddress();
								System.out.println(containerId+":"+hostName+","+DiagnosticsInfo+","+logurlString+","+NodeHttpAddress);

								ContarinerInfos contarinerInfos = new ContarinerInfos();
								contarinerInfos.containerId = containerId + "";
								contarinerInfos.allocatedCpu = allocatedResource.getVirtualCores();
								contarinerInfos.allocatedMemory = allocatedResource.getMemory();
								contarinerInfos.state = containerState + "";
								contarinerInfosList.add(contarinerInfos);
							}
						}
					}
				}
			}
		}
	}
	
	/**
	 * 得到yarn
	 * LANG
	 * @return
	 */
	public YarnClient getYarnClient(){
		YarnConfiguration conf = new YarnConfiguration();
//		conf.set("fs.defaultFS", "hdfs://rtask-nod8:8020");
		conf.set("yarn.resourcemanager.scheduler.address", "rtask-nod8:8030");
		String hadoopHome = System.getenv("HADOOP_HOME");
		conf.addResource(new Path(hadoopHome + "/etc/hadoop/core-site.xml"));
		conf.addResource(new Path(hadoopHome + "/etc/hadoop/yarn-site.xml"));
		YarnClient yarnClient = YarnClient.createYarnClient();
		yarnClient.init(conf);
		yarnClient.start();
		return yarnClient;
	}
	
	/**
	 * 将该方法放在spark或者hadoop的分布式程序中，可以得到该程序在每台节点上的pid
	 */
	public int getPID() {
	    String processName = java.lang.management.ManagementFactory.getRuntimeMXBean().getName();
	    if (processName != null && processName.length() > 0) {
	        try {
	            return Integer.parseInt(processName.split("@")[0]);
	        }
	        catch (Exception e) {
	            return 0;
	        }
	    }

	    return 0;
	}
}
