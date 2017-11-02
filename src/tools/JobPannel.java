package tools;

import etl.ConvertHBaseKeyJob;
import etl.ConvertShixinKeyJob;
import etl.DeleteFromLengJingGSJob;
import etl.DeleteWrongKeyJob;
import etl.FixShareholderTypeJob;
import etl.HiveToHbaseJob;
import etl.MergePersonNodesJob;
import etl.TransTextToSequenceFile;
import etl.UpdateBianGengJob;
import etl.UpdateBianGengJob2;
import etl.UpdateChangeFormatJob;
import etl.UpdateJingYingQiXianJob;
import kafka.AddComanyNameToKafkaJob;
import neo4j.UpdateNeo4jFromMySQL;
import neo4j.UpdateRiskModelJob;
import notice.CourtNoticeType1Job;
import notice.CourtNoticeType2Job;
import shuashuju.GuangDongBianGeng;
import shuashuju.GuangDongBianGeng2;

public class JobPannel {

	public static void run(String[] args) throws Exception
	{
		JobConfig jobConf=new JobConfig(args);
		if(jobConf.jobName.equals("HiveToHbase"))
		{
			new HiveToHbaseJob().run(jobConf);
		}
		else if(jobConf.jobName.equals("DeleteFromLengJingGS"))
		{
			new DeleteFromLengJingGSJob().run(jobConf);
		}
		else if(jobConf.jobName.equals("FixShareholderType"))
		{
			new FixShareholderTypeJob().run();
		}
		else if(jobConf.jobName.equals("MapReduceReadFile"))
		{
			new MapReduceReadFile().run();
		}
		else if(jobConf.jobName.equals("TransTextToSequenceFile"))
		{
			new TransTextToSequenceFile().run(jobConf);
		}
		else if(jobConf.jobName.equals("MergePersonNodesJob"))
		{
			new MergePersonNodesJob().run();
		}
		else if(jobConf.jobName.equals("CourtNoticeType1Job"))
		{
			CourtNoticeType1Job job =new CourtNoticeType1Job();
			job.run(jobConf);
		}
		else if(jobConf.jobName.equals("CourtNoticeType2Job"))
		{
			CourtNoticeType2Job job =new CourtNoticeType2Job();
			job.run(jobConf);
		}
		else if(jobConf.jobName.equals("Test"))
		{
			new Test().test();
		}
		else if(jobConf.jobName.equals("ConvertHBaseKeyJob"))
		{
			new ConvertHBaseKeyJob().run(jobConf);
		}
		else if(jobConf.jobName.equals("UpdateChangeFormatJob"))
		{
			new UpdateChangeFormatJob().run(jobConf);
		}
		else if(jobConf.jobName.equals("ConvertShixinKeyJob"))
		{
			new ConvertShixinKeyJob().run(jobConf);
		}
		else if(jobConf.jobName.equals("DeleteWrongKeyJob"))
		{
			new DeleteWrongKeyJob().run(jobConf);
		}
		else if(jobConf.jobName.equals("UpdateNeo4jFromMySQL"))
		{
			UpdateNeo4jFromMySQL job=new UpdateNeo4jFromMySQL();
			job.updateFromFile();
		}
		else if(jobConf.jobName.equals("UpdateRiskModelJob"))
		{
			new UpdateRiskModelJob().run();
		}
		else if(jobConf.jobName.equals("UpdateBianGengJob"))
		{
			new UpdateBianGengJob2().run();
		}
		else if(jobConf.jobName.equals("UpdateJingYingQiXianJob"))
		{
			new UpdateJingYingQiXianJob().run(jobConf);
		}
		else if(jobConf.jobName.equals("GuangDongBianGeng2"))
		{
			GuangDongBianGeng2.run();
		}
		else if(jobConf.jobName.equals("AddComanyNameToKafkaJob"))
		{
			 new AddComanyNameToKafkaJob().run();
		}
		else
		{
			System.out.println("Please input right jobName!");
		}
	}
	
	public static void main(String[] args) throws Exception
	{
//		args=new String[]{"--jobName=LnGsUpdateJob"};
//		args=new String[]{"--jobName=NacaoOrg"};
//		args=new String[]{"--jobName=BaiduApp","--startCode=690000000"};
//		args=new String[]{"--jobName=Test"};
//		args=new String[]{"--jobName=AddNewToGraph", "--srcTable=base","--dt=2016-04-21","--batchSize=10"};
//		args=new String[]{"--jobName=FixShareholderType"};
		run(args);
	}
}
