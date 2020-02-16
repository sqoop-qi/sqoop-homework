package com.qi.util;

import java.util.List;

import org.apache.sqoop.client.SqoopClient;
import org.apache.sqoop.model.MConfig;
import org.apache.sqoop.model.MFromConfig;
import org.apache.sqoop.model.MInput;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.model.MLink;
import org.apache.sqoop.model.MLinkConfig;
import org.apache.sqoop.model.MSubmission;
import org.apache.sqoop.model.MToConfig;
import org.apache.sqoop.validation.Status;

public class MysqlToHdfsByCondition {

	public static final String SQOOP_URL = "http://master:12000/sqoop/";
	public static SqoopClient client = new SqoopClient(SQOOP_URL);

	// 创建一个link来连接linux上的mysql
	public static void createLinkFromLinuxMysql() {
		MLink mysqlLink = client.createLink("generic-jdbc-connector");
		mysqlLink.setName("sqoop-homework-link");
		mysqlLink.setCreationUser("root");

		MLinkConfig linkConfig = mysqlLink.getConnectorLinkConfig();
		linkConfig.getStringInput("linkConfig.connectionString").setValue("jdbc:mysql://master:3306/sqoop_test");
		linkConfig.getStringInput("linkConfig.jdbcDriver").setValue("com.mysql.jdbc.Driver");
		linkConfig.getStringInput("linkConfig.username").setValue("root");
		linkConfig.getStringInput("linkConfig.password").setValue("123456");
		linkConfig.getStringInput("dialect.identifierEnclose").setValue("`");

		List<MConfig> configs = linkConfig.getConfigs();
		describeConfigs(configs);
		MLink link = client.getLink("sqoop-homework-link");

		if (link == null) {
			Status status = client.saveLink(mysqlLink);
			if (status.canProceed()) {
				System.out.println("创建link成功");
			} else {
				System.out.println("创建link失败" + status.toString());
			}
		}else {
			System.out.println("link已存在");
		}

	}

	public static void createJobMysqlToHdfs(String date) {
		MJob job = client.createJob("sqoop-homework-link", "my-hdfs");
		job.setName("sqoop-homework-job");
		MFromConfig fromJobConfig = job.getFromJobConfig();
		MToConfig toJobConfig = job.getToJobConfig();
		System.out.println("打印from job的配置参数");
		describeConfigs(fromJobConfig.getConfigs());
		// fromJobConfig.getStringInput("fromJobConfig.schemaName").setValue("sqoop_test");
		// fromJobConfig.getStringInput("fromJobConfig.tableName").setValue("order_custom");

		String sql = String.format(
				"SELECT orderid ,customerid,orderdate from order_custom WHERE orderdate=%s and ${CONDITIONS}", date);
		fromJobConfig.getStringInput("fromJobConfig.sql").setValue(sql);
		fromJobConfig.getStringInput("fromJobConfig.partitionColumn").setValue("orderid");
		System.out.println("打印to job的配置参数");
		describeConfigs(toJobConfig.getConfigs());
		toJobConfig.getStringInput("toJobConfig.outputDirectory").setValue("/sqoop-homework/" + date);
		toJobConfig.getEnumInput("toJobConfig.outputFormat").setValue("TEXT_FILE");
		toJobConfig.getBooleanInput("toJobConfig.appendMode").setValue(true);

		client.deleteJob("sqoop-homework-job");
		Status status = client.saveJob(job);
		if (status.canProceed()) {
			System.out.println("创建job成功");
		} else {
			System.out.println("job创建失败" + status.toString());
		}

	}

	// 打印配置项
	public static void describeConfigs(List<MConfig> configs) {
		for (MConfig mConfig : configs) {
			List<MInput<?>> inputs = mConfig.getInputs();
			for (MInput<?> mInput : inputs) {
				System.out.println(mInput);
			}
		}
	}

	public static void startJob(String jobName) {
		MSubmission submission = client.startJob(jobName);
		while (submission.getStatus().isRunning()) {
			System.out.println("程序运行中请稍候");
			try {
				Thread.sleep(3000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

		}
		if (submission.getStatus().isFailure()) {
			System.out.println("job运行失败");
		} else {
			System.out.println("job运行成功");

		}
	}

	public static void main(String[] args) {
		 createLinkFromLinuxMysql();
		 createJobMysqlToHdfs("20180320");
		startJob("sqoop-homework-job");
	}
}
