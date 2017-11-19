package org.cripac.isee.vpe.ctrl;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.cripac.isee.vpe.entities.Report;
import org.cripac.isee.vpe.entities.Report.ClusterInfo.ApplicationInfos;
import org.cripac.isee.vpe.entities.Report.ClusterInfo.Nodes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;
import org.cripac.isee.vpe.util.kafka.KafkaHelper;
import com.google.gson.Gson;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

public class KafkaConsumer 
{
	private final Logger log = LoggerFactory.getLogger(KafkaConsumer.class);
	public static final String REPORT_TOPIC = "monitor-desc-";
	private String topic;
	private	static Configuration conf;
	private static FileSystem fs;
	private KafkaProducer<String, String> reportProducer;
	
	public KafkaConsumer(String topic){  
        super();  
        this.topic = topic;  
    }

	
	public String getTopic() {
		return topic;
	}


	public void setTopic(String topic) {
		this.topic = topic;
	}


	public KafkaConsumer() {
		super();
		// TODO Auto-generated constructor stub
	}


	public void report(String topic,Report reportAll,ConsumerConnector consumer,FileSystem hdfs) throws IOException, URISyntaxException, ParserConfigurationException, SAXException {
		
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(topic, 1); // 一次从主题中获取一个数据
		Map<String, List<KafkaStream<byte[], byte[]>>> messageStreams = consumer.createMessageStreams(topicCountMap);
		KafkaStream<byte[], byte[]> stream = messageStreams.get(topic).get(0);// 获取每次接收到的这个数据
		ConsumerIterator<byte[], byte[]> iterator = stream.iterator();
		while (iterator.hasNext()) {
			String message = new String(iterator.next().message());
			System.out.println("接收到的监控信息是: " + message);
			Report report=new Gson().fromJson(message,Report.class);
			reportAll.serverInfosMap.putAll(report.serverInfosMap);
			reportAll.clusterInfo.nodeInfosList.addAll(report.clusterInfo.nodeInfosList);
			for (int i = 0; i < report.clusterInfo.applicationInfosList.size(); i++) {
				ApplicationInfos applicationInfos=report.clusterInfo.applicationInfosList.get(i);
				ApplicationInfos applicationInfosAll=new ApplicationInfos();
				applicationInfosAll.applicationId=applicationInfos.applicationId;
				applicationInfosAll.neededResourceMemory=applicationInfos.neededResourceMemory;
				applicationInfosAll.neededResourceVcore=applicationInfos.neededResourceVcore;
				applicationInfosAll.reservedResourceMemory=applicationInfos.reservedResourceMemory;
				applicationInfosAll.reservedResourceVcore=applicationInfos.reservedResourceVcore;
				applicationInfosAll.usedResourceMemory=applicationInfos.usedResourceMemory;
				applicationInfosAll.usedResourceVcore=applicationInfos.usedResourceVcore;
				applicationInfosAll.contarinerInfosList.addAll(applicationInfos.contarinerInfosList);
				applicationInfosAll.eachAppNodeMap.putAll(applicationInfos.eachAppNodeMap);
				reportAll.clusterInfo.applicationInfosList.add(applicationInfosAll);
				
			}
			
//			List<ApplicationInfos> appList=report.clusterInfo.applicationInfosList;
//			log.info("接收到的app编号是: " + appList.toString());
			//
			send(reportAll);
			
			String storeDir="/user/vpe.cripac";
			storeJson(hdfs,storeDir,new Gson().toJson(reportAll));
		}
	}

	private ConsumerConnector createConsumer() {
		Properties properties = new Properties();
		properties.put("zookeeper.connect", "gpu-task-nod1:2181,gpu-task-nod2:2181,gpu1608:2181");// 声明zk
		properties.put("group.id", "kafkaCus");// 必须要使用别的组名称，
												// 如果生产者和消费者都在同一组，则不能访问同一组内的topic数据
		return Consumer.createJavaConsumerConnector(new ConsumerConfig(properties));
	}

	public static void main(String[] args) throws YarnException, IOException, URISyntaxException, ParserConfigurationException, SAXException {
//		String ipString;
//        try {
//        	ipString=WebToolUtils.getLocalIP();
//		} catch (Exception e) {
//			// TODO: handle exception
//			ipString="Unknown ip";
//		}
		
		KafkaConsumer kafkaConsumer =new KafkaConsumer();
		List<String> nodeNamesList=kafkaConsumer.getNodesName(kafkaConsumer.getYarnClient());
		Report reportAll=new Report();
		ConsumerConnector consumer = kafkaConsumer.createConsumer();
		FileSystem hdfs=kafkaConsumer.HDFSOperation();
		for (int i = 0; i < nodeNamesList.size(); i++) {
			
			kafkaConsumer.report(nodeNamesList.get(i),reportAll,consumer,hdfs);// 使用kafka集群中创建好的主题 test
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
	 * 为了方便，加入了topic的前缀
	 * LANG
	 * @param yarnClient
	 * @return
	 * @throws YarnException
	 * @throws IOException
	 */
	public List<String> getNodesName(YarnClient yarnClient) throws YarnException, IOException{
		List<NodeReport> nodeList=yarnClient.getNodeReports(NodeState.RUNNING);
		List<String> nodeNamesList=new ArrayList<String>();
		for (int i = 0; i < nodeList.size(); i++) {
			
			NodeReport nodeReport=nodeList.get(i);
			nodeNamesList.add(REPORT_TOPIC+nodeReport.getNodeId().getHost());
			
		}
		return nodeNamesList;
	}
	
	
	
	public FileSystem HDFSOperation() throws IOException{
		conf = new Configuration();
		conf.set("fs.default.name", "hdfs://172.18.33.84:8020");
      String hadoopHome = System.getenv("HADOOP_HOME");
      conf.addResource(new Path(hadoopHome + "/etc/hadoop/core-site.xml"));
      conf.addResource(new Path(hadoopHome + "/etc/hadoop/yarn-site.xml"));
      conf.setBoolean("dfs.support.append", true);
//      conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName(), "LaS-VPE-Platform-Web");
//      conf.set("fs.file.impl", LocalFileSystem.class.getName(), "LaS-VPE-Platform-Web");
		fs = FileSystem.get(conf);
		return fs;
	}
	
	public void storeJson(FileSystem hdfs,String storeDir,String reportAllJson) throws IllegalArgumentException, IOException{
		final FSDataOutputStream outputStream = hdfs.create(new Path(storeDir + "/monitor.txt"));
		outputStream.writeBytes(reportAllJson);
		outputStream.flush();
		outputStream.close();
		
	}
	
	public void send(Report reportAll) throws URISyntaxException, ParserConfigurationException, SAXException{
		SystemPropertyCenter propCenter = new SystemPropertyCenter("send report");
		this.reportProducer = new KafkaProducer<>(propCenter.getKafkaProducerProp(true));
		KafkaHelper.createTopic(propCenter.zkConn, propCenter.zkSessionTimeoutMs, propCenter.zkConnectionTimeoutMS,
                REPORT_TOPIC+"all",
                propCenter.kafkaNumPartitions, propCenter.kafkaReplFactor);
		this.reportProducer.send(new ProducerRecord<>(REPORT_TOPIC+"all", "all", new Gson().toJson(reportAll)));
	}
}
