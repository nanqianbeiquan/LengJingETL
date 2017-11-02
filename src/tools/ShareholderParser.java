package tools;

import java.util.HashMap;
import java.util.HashSet;

public class ShareholderParser {

	public static HashMap<String,String> gdlxMap=new HashMap<String,String>();
	public static HashMap<String,String> zjlxMap=new HashMap<String,String>();
	public static HashSet<String> unknownGdNameSet=new HashSet<String>();
	public static HashSet<String> unknownLabelSet=new HashSet<String>();
	static 
	{
		gdlxMap.put("香港居民","Person");
		gdlxMap.put("外商投资创业投资企业","Company");
		gdlxMap.put("国家授权投资的机构或部门","Company");
		gdlxMap.put("其他自然人投资者","Person");
		gdlxMap.put("华侨","Person");
		gdlxMap.put("台湾居民","Person");
		gdlxMap.put("外国公民","Person");
		gdlxMap.put("社会团体法人","Company");
		gdlxMap.put("外商/港澳台投资企业","Company");
		gdlxMap.put("外商投资合伙企业","Company");
		gdlxMap.put("民办非企业单位","Company");
		gdlxMap.put("合伙人","Person");
		gdlxMap.put("工会法人","Company");
		gdlxMap.put("内资合伙企业","Company");
		gdlxMap.put("外国（地区）自然人","Person");
		gdlxMap.put("外国(地区)经济组织","Company");
		gdlxMap.put("外国（地区）自然人","Person");
		gdlxMap.put("非农民自然人","Person");
		gdlxMap.put("自然人股东","Person");
		gdlxMap.put("境内中国公民","Person");
		gdlxMap.put("自然人","Person");
		gdlxMap.put("外籍自然人","Person");
		gdlxMap.put("企业法人","Company");
		gdlxMap.put("合伙企业","Company");
		gdlxMap.put("外商投资企业","Company");
		gdlxMap.put("个人独资企业","Company");
		gdlxMap.put("外商投资投资性公司","Company");
		gdlxMap.put("外国(地区)企业","Company");
		gdlxMap.put("有限公司","Company");
		gdlxMap.put("有限公司(自然人控股)","Company");
		gdlxMap.put("股份有限公司(自然人控股)","Company");
		gdlxMap.put("有限责任公司(中外合资)","Company");
		gdlxMap.put("有限公司(法人独资)内资","Company");
		gdlxMap.put("股份有限公司(上市,自然人控股)","Company");
		gdlxMap.put("有限公司(国有独资)","Company");
		gdlxMap.put("有限责任公司(台港澳法人独资)","Company");
		gdlxMap.put("有限责任公司(台港澳与境内合资)","Company");
		gdlxMap.put("有限公司(法人独资)内资","Company");
		gdlxMap.put("有限公司(法人独资)内资","Company");
		gdlxMap.put("其他非自然人投资者","Company");
		gdlxMap.put("法人股东","Company");
		gdlxMap.put("个人独资","Company");
		gdlxMap.put("事业法人","Company");
		gdlxMap.put("社团法人","Company");
		gdlxMap.put("法人","Company");
		gdlxMap.put("全民所有制","Company");
		gdlxMap.put("机关法人","Company");
		gdlxMap.put("农民自然人","Person");
		gdlxMap.put("母公司","Company");
		gdlxMap.put("中方（承料方）","Company");
		gdlxMap.put("参股公司","Company");
		gdlxMap.put("商务单位","Company");
		gdlxMap.put("外方（来料方）","Company");
		gdlxMap.put("港澳台投资者","Company");
		/*
		 * 其他投资者
		 */
		zjlxMap.put("澳门（永久性）居民身份证","Person");
		zjlxMap.put("不需要登记的社团法人","Company");
		zjlxMap.put("出入境管理证","Person");
		zjlxMap.put("非法人企业、个体工商户","Company");
		zjlxMap.put("非公司制企业法人执照","Company");
		zjlxMap.put("分公司执照","Company");
		zjlxMap.put("港澳居民来往内地通行证","Person");
		zjlxMap.put("个人独资企业营业执照","Company");
		zjlxMap.put("个人独资执照","Company");
		zjlxMap.put("个人合伙执照","Company");
		zjlxMap.put("个体工商户营业执照","Company");
		zjlxMap.put("公司执照","Company");
		zjlxMap.put("合伙企业分支机构营业执照","Company");
		zjlxMap.put("合伙企业营业执照","Company");
		zjlxMap.put("户口薄","Person");
		zjlxMap.put("回乡证","Person");
		zjlxMap.put("机关法人登记证","Company");
		zjlxMap.put("集团登记证","Company");
		zjlxMap.put("居民证","Person");
		zjlxMap.put("内资企业法人","Company");
		zjlxMap.put("农民专业合作社法人营业执照","Company");
		zjlxMap.put("农民专业合作社分支机构营业执照","Company");
		zjlxMap.put("农业专业合作社","Company");
		zjlxMap.put("农业专业合作社营业执照","Company");
		zjlxMap.put("其他有效身份证件","Person");
		zjlxMap.put("其它有效身份证件","Person");
		zjlxMap.put("企业法人营业执照(非公司);","Company");
		zjlxMap.put("企业法人营业执照(公司);","Company");
		zjlxMap.put("企业法人营业执照(外资);","Company");
		zjlxMap.put("企业集体登记证","Company");
		zjlxMap.put("企业集团登记证","Company");
		zjlxMap.put("社会团体法人证","Company");
		zjlxMap.put("社团法人登记证","Company");
		zjlxMap.put("身份证","Person");
		zjlxMap.put("事业单位法人证","Company");
		zjlxMap.put("事业法人登记证","Company");
		zjlxMap.put("台胞证","Person");
		zjlxMap.put("台湾居民来往大陆通行证","Person");
		zjlxMap.put("台湾居民身份证","Person");
		zjlxMap.put("通行证","Person");
		zjlxMap.put("外国（地区）护照","Person");
		zjlxMap.put("外国(地区);企业常驻代表机构登记证","Company");
		zjlxMap.put("外商投资合伙企业营业执照","Company");
		zjlxMap.put("外商投资企业办事机构注册证","Company");
		zjlxMap.put("外商投资企业执照","Company");
		zjlxMap.put("香港（永久性）居民身份证","Person");
		zjlxMap.put("香港居民身份证","Person");
		zjlxMap.put("营业执照","Company");
		zjlxMap.put("营业执照(分公司、营业单位)","Company");
		zjlxMap.put("营业执照(外资);","Company");
		zjlxMap.put("营业执照（新版）","Company");
		zjlxMap.put("中国护照","Person");
		zjlxMap.put("中华人民共和国警官证","Person");
		zjlxMap.put("中华人民共和国居民身份证","Person");
		zjlxMap.put("中华人民共和国军官证","Person");
		
		unknownGdNameSet.add("国务院");
		unknownGdNameSet.add("国家");
		unknownGdNameSet.add("外交部");
		unknownGdNameSet.add("国防部");
		unknownGdNameSet.add("发展改革委");
		unknownGdNameSet.add("发改委");
		unknownGdNameSet.add("教育部");
		unknownGdNameSet.add("科技部");
		unknownGdNameSet.add("国防科工委");
		unknownGdNameSet.add("国家民委");
		unknownGdNameSet.add("公安部");
		unknownGdNameSet.add("安全部");
		unknownGdNameSet.add("监察部");
		unknownGdNameSet.add("民政部");
		unknownGdNameSet.add("司法部");
		unknownGdNameSet.add("财政部");
		unknownGdNameSet.add("人事部");
		unknownGdNameSet.add("劳动保障部");
		unknownGdNameSet.add("国土资源部");
		unknownGdNameSet.add("建设部");
		unknownGdNameSet.add("铁道部");
		unknownGdNameSet.add("交通部");
		unknownGdNameSet.add("信息产业部");
		unknownGdNameSet.add("水利部");
		unknownGdNameSet.add("农业部");
		unknownGdNameSet.add("商务部");
		unknownGdNameSet.add("文化部");
		unknownGdNameSet.add("卫生部");
		unknownGdNameSet.add("人口计生委");
		unknownGdNameSet.add("计生委");
		unknownGdNameSet.add("人民银行");
		unknownGdNameSet.add("审计署");
		unknownGdNameSet.add("海关总署");
		unknownGdNameSet.add("税务总局");
		unknownGdNameSet.add("工商总局");
		unknownGdNameSet.add("质检总局");
		unknownGdNameSet.add("环保总局");
		unknownGdNameSet.add("民航总局");
		unknownGdNameSet.add("广电总局");
		unknownGdNameSet.add("新闻出版总署");
		unknownGdNameSet.add("版权局");
		unknownGdNameSet.add("体育总局");
		unknownGdNameSet.add("安全监管总局");
		unknownGdNameSet.add("统计局");
		unknownGdNameSet.add("林业局");
		unknownGdNameSet.add("食品药品监管局");
		unknownGdNameSet.add("知识产权局");
		unknownGdNameSet.add("旅游局");
		unknownGdNameSet.add("宗教局");
		unknownGdNameSet.add("参事室");
		unknownGdNameSet.add("国管局");
		unknownGdNameSet.add("侨办");
		unknownGdNameSet.add("港澳办");
		unknownGdNameSet.add("法制办");
		unknownGdNameSet.add("国研室");
		unknownGdNameSet.add("台办");
		unknownGdNameSet.add("新闻办");
		unknownGdNameSet.add("国务院台湾事务办公室与中共中央台湾工作办公室");
		unknownGdNameSet.add("国务院新闻办公室与中共中央对外宣传办公室");
		unknownGdNameSet.add("新华社");
		unknownGdNameSet.add("中科院");
		unknownGdNameSet.add("社科院");
		unknownGdNameSet.add("工程院");
		unknownGdNameSet.add("发展研究中心");
		unknownGdNameSet.add("行政学院");
		unknownGdNameSet.add("地震局");
		unknownGdNameSet.add("气象局");
		unknownGdNameSet.add("银监会");
		unknownGdNameSet.add("证监会");
		unknownGdNameSet.add("保监会");
		unknownGdNameSet.add("电监会");
		unknownGdNameSet.add("社保基金会");
		unknownGdNameSet.add("自然科学基金会");
		unknownGdNameSet.add("信访局");
		unknownGdNameSet.add("粮食局");
		unknownGdNameSet.add("烟草局");
		unknownGdNameSet.add("外专局");
		unknownGdNameSet.add("海洋局");
		unknownGdNameSet.add("测绘局");
		unknownGdNameSet.add("邮政局");
		unknownGdNameSet.add("文物局");
		unknownGdNameSet.add("中医药局");
		unknownGdNameSet.add("外汇局");
		unknownGdNameSet.add("煤矿安监局");
		unknownGdNameSet.add("档案局");
		unknownGdNameSet.add("保密局");
		unknownGdNameSet.add("国家档案局与中央档案馆");
		unknownGdNameSet.add("国家保密局与中央保密委员会办公室");
		
		unknownLabelSet.add("有限售条件流通股");
		unknownLabelSet.add("无限售条件流通股");
		unknownLabelSet.add("社会公众股");
		unknownLabelSet.add("非发起人股");
		unknownLabelSet.add("非公开发行股");
		unknownLabelSet.add("0");
		unknownLabelSet.add("1");
		unknownLabelSet.add("2");
		unknownLabelSet.add("3");
		unknownLabelSet.add("4");
		unknownLabelSet.add("5");
		unknownLabelSet.add("6");
		unknownLabelSet.add("7");
		unknownLabelSet.add("8");
		unknownLabelSet.add("9");
		unknownLabelSet.add("10");
	}
	
	public static String getShareHolderType(String gdlx,String zjlx,String gdmc)
	{
		if(unknownLabelSet.contains(gdmc))
		{
			return "Unknown";
		}
		String shareholderType=null;
		if(shareholderType==null && gdlx!=null && gdlxMap.containsKey(gdlx))
		{
			shareholderType=gdlxMap.get(gdlx);
		}
		if(shareholderType==null && zjlx!=null && zjlxMap.containsKey(zjlx))
		{
			shareholderType=zjlxMap.get(gdlx);
		}
		if(shareholderType==null)
		{
			if(gdmc.contains("公司")
					|| gdmc.contains("集团")
					|| gdmc.contains("企业")
					|| gdmc.contains("超市")
					|| gdmc.contains("合作社")
					|| gdmc.contains("养殖场")
					|| gdmc.contains("营业部")
					|| gdmc.contains("软件行")
					|| gdmc.contains("杂志社")
					|| gdmc.contains("出版社")
					|| gdmc.contains("委员会")
					|| (gdmc.length()>=4 && (
							gdmc.endsWith("厂")
							|| gdmc.endsWith("社")
							|| gdmc.endsWith("店")
							|| gdmc.contains("合伙")
							|| gdmc.contains("中心")
							|| gdmc.contains("部")
						))
					)
					{
						shareholderType="Company";
					}
			else if(unknownGdNameSet.contains(gdmc))
			{
				shareholderType="Company";
			}
			else if(gdmc.length()<=3)
			{
				shareholderType="Person";
			}
			else
			{
				shareholderType="Unknown";
			}
		}
		return shareholderType;
	}
	
	public static void main(String[] args)
	{
	
	}
}
