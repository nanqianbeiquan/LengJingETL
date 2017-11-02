package tools;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;

public class ReadRegionInfos {

	public ReadRegionInfos() throws MasterNotRunningException, ZooKeeperConnectionException, IOException
	{
		Configuration HBaseConfig = HBaseConfiguration.create(new Configuration());
		HBaseAdmin admin=new HBaseAdmin(HBaseConfig);
		List<HRegionInfo> regions = admin.getTableRegions(Bytes.toBytes("GS"));
		StringBuilder splits=new StringBuilder();
		for(HRegionInfo region:regions)
		{
			//			System.out.println(Bytes.toString(region.getEndKey()));
			splits.append("'"+Bytes.toString(region.getStartKey())+"',");
		}
		if(splits.length()>0)
		{
			splits.setLength(splits.length()-1);
		}
		System.out.println(splits);

	}
	public static void main(String[] args) throws MasterNotRunningException, ZooKeeperConnectionException, IOException
	{
		ReadRegionInfos job=new ReadRegionInfos();
	}
	
	/*
	 * create 'LengJingGS2',{NAME=>COMPRESSION=>'GZ',VERSIONS=>5},{SPLITS=>['�Ϻ�����ó�����޹�˾_01_','�Ϻ����ɵ����������޹�˾_01_','�Ϻ����������Ƽҹ������޹�˾_01_','�Ϻ����˻����豸��װ���޹�˾_01_','�������˻���ó�������ι�˾_01_','��ɽ�д�ӿ��κ�Ҽʰ����װ���ճ�_01_','��³ľ�������������ܿƼ���������_01_','��Ҧ�н���������޹�˾_01_','�����λ��Ͷ�����޹�˾_01_','�����Ŷ��߿ƿƼ���չ���޹�˾_01_','�����෽ͨ��ҵ�ά������_01_','���������ز������޹�˾_01_','����������ҵͶ�ʹ������޹�˾_01_','�����������޹���������ι�˾_01_','�����𿵸߿ƹ���ó�����޹�˾_01_','������ʢ���ز��������޹�˾_01_','�Ͼ����ƶپƵ�������޹�˾_01_','�ϲ����Ŷ��������޹�˾_01_','����������������޹�˾_01_','̨����·�Ż���ʯ�ľ�ó���޹�˾_01_','������Ԫ������Ʒ���޹�˾_01_','�Ĵ����Ƶ��ӿƼ����޹�˾_01_','�����θ���ó�����޹�˾_01_','���Ϫ�ڼ��β�Ϳװ��_01_','��ƽ������˿����Ʒ���޹�˾_01_','ɽ���г���ó�����޹�˾_01_','����������ҵ���޹�˾_01_','����������Я��ͨѶ���ľ�Ӫ��_01_','�ȷ�����˼����ó���޹�˾_01_','�������ï����ũ��רҵ������_01_','ɣֲ������Һ�������޹�˾_01_','���ϸ�ϼ��ó���޹�˾_01_','���Ϲ���ΰҵ�����Ƽ����޹�˾_01_','������Ե¡ʵҵ���޹�˾_01_','�ػʵ���?','����ʱ����ǿ�ֽṹ���޹�˾_01_','֣�ݺ��ո���������ҵ�������޹�˾_01_','�����к�������������������޹�˾_04_glad23459035','��ԭ����̩��ҵ���޹�˾_01_']}
	 */
}
