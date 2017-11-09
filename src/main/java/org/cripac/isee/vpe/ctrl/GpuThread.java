package org.cripac.isee.vpe.ctrl;

import java.io.IOException;
import java.util.EnumSet;
import java.util.List;

import javax.annotation.Nonnull;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationResourceUsageReport;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.cripac.isee.util.Singleton;
import org.cripac.isee.vpe.util.logging.Logger;
import org.cripac.isee.vpe.util.logging.SynthesizedLogger;
import org.cripac.isee.vpe.util.logging.SynthesizedLoggerFactory;
import org.slf4j.LoggerFactory;


public class GpuThread {

	Singleton<Logger> loggerSingleton;
//	 private static final org.slf4j.Logger log = LoggerFactory.getLogger(GpuThread.class);
//	@Nonnull
//	private SystemPropertyCenter propCenter;
//	@Nonnull
//	private final String appName;

	public GpuThread(@Nonnull SystemPropertyCenter propCenter, @Nonnull String appName) throws Exception {
//		this.propCenter = propCenter;
//		this.appName = appName;
		//appName 用来check topic
		this.loggerSingleton = new Singleton<>(new SynthesizedLoggerFactory(appName, propCenter),
				SynthesizedLogger.class);
		MonitorThread monitorThread = new MonitorThread(loggerSingleton.getInst(), propCenter);
		monitorThread.start();
	}

	public static void main(String[] args) throws Exception {
		String java_class_path=System.getProperty("java.class.path");
		System.out.println("初始的："+"java.class.path:"+java_class_path);
		String java_library_path=System.getProperty("java.library.path");
		System.out.println("初始的："+"java.library.path:"+java_library_path);
        SystemPropertyCenter propCenter = new SystemPropertyCenter(args);
        String appName="monitor-cluster";
        GpuThread gpuThread=new GpuThread(propCenter, appName);
        Logger logger=gpuThread.loggerSingleton.getInst();
        logger.info("java.class.path:"+java_class_path);
        logger.info("java.library.path:"+java_library_path);
	}
	
}
