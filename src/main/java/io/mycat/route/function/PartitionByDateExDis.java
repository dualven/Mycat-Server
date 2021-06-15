package io.mycat.route.function;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import org.slf4j.Logger; import org.slf4j.LoggerFactory;

import io.mycat.config.model.rule.RuleAlgorithm;

/**
 * 例子 按日期列分区  格式 between操作解析的范例
 * 
 * @author dualven 如果》365 ，则取模  比如  366 则放在第一上
 * 这个是分布式版本，如果库分在多个主机上，则可以不按顺序，而是按间隔分配，
 * 比如之前 是，1，2，3，4，5,...., 364,365
 * 现在可能 变成，1，
 * 
 */
public class PartitionByDateExDis extends AbstractPartitionAlgorithm implements RuleAlgorithm {
	private static final Logger LOGGER = LoggerFactory.getLogger(PartitionByDateExDis.class);

	private String sBeginDate;
	private String sEndDate;
	private String sPartionDay;
	private String sDisPart;
	private String dateFormat;

	private long beginDate;
	private long partionTime;
	private long endDate;
	private int nCount;
	private int nPart;

	private ThreadLocal<SimpleDateFormat> formatter;
	
	private static final long oneDay = 86400000;

	@Override
	public void init() {
		try {
			partionTime = Integer.parseInt(sPartionDay) * oneDay;
			nPart = Integer.parseInt(sDisPart);
			beginDate = new SimpleDateFormat(dateFormat).parse(sBeginDate).getTime();

			if(sEndDate!=null&&!sEndDate.equals("")){
			    endDate = new SimpleDateFormat(dateFormat).parse(sEndDate).getTime();
			    nCount = (int) ((endDate - beginDate) / partionTime) + 1;
			}
			 
			formatter = new ThreadLocal<SimpleDateFormat>() {
				@Override
				protected SimpleDateFormat initialValue() {
					return new SimpleDateFormat(dateFormat);
				}
			};
		} catch (ParseException e) {
			throw new java.lang.IllegalArgumentException(e);
		}
	}

	@Override
	public Integer calculate(String columnValue)  {
		try {
			long targetTime = formatter.get().parse(columnValue).getTime();
			int targetPartition = (int) ((targetTime - beginDate) / partionTime);

			if(targetTime>endDate && nCount!=0){
				targetPartition = targetPartition%nCount;
			}
			if(targetPartition >= 365) {
				targetPartition = targetPartition%365;
			}

			// int everypart = (int)(nCount/nPart);
			//原划分
			//targetPartition = ((int)(targetPartition/everypart))*everypart + targetPartition%everypart;
			targetPartition = ((int)(nCount/nPart))*(targetPartition%nPart) + (int)(targetPartition/nPart);
			return targetPartition;

		} catch (ParseException e) {
			throw new IllegalArgumentException(new StringBuilder().append("columnValue:").append(columnValue).append(" Please check if the format satisfied.").toString(),e);
		}
	}

	@Override
	public Integer[] calculateRange(String beginValue, String endValue)  {
		SimpleDateFormat format = new SimpleDateFormat(this.dateFormat);
		try {
			Date beginDate = format.parse(beginValue);
			Date endDate = format.parse(endValue);
			Calendar cal = Calendar.getInstance();
			List<Integer> list = new ArrayList<Integer>();
			while(beginDate.getTime() <= endDate.getTime()){
				Integer nodeValue = this.calculate(format.format(beginDate));
				if(Collections.frequency(list, nodeValue) < 1) list.add(nodeValue);
				cal.setTime(beginDate);
				cal.add(Calendar.DATE, 1);
				beginDate = cal.getTime();
			}

			Integer[] nodeArray = new Integer[list.size()];
			for (int i=0;i<list.size();i++) {
				nodeArray[i] = list.get(i);
			}

			return nodeArray;
		} catch (ParseException e) {
			LOGGER.error("error",e);
			return new Integer[0];
		}
	}
	
	@Override
	public int getPartitionNum() {
		int count = this.nCount;
		return count > 0 ? count : -1;
	}

	public void setsBeginDate(String sBeginDate) {
		this.sBeginDate = sBeginDate;
	}

	public void setsPartionDay(String sPartionDay) {
		this.sPartionDay = sPartionDay;
	}

	public void setDateFormat(String dateFormat) {
		this.dateFormat = dateFormat;
	}
	public String getsEndDate() {
		return this.sEndDate;
	}
	public void setsEndDate(String sEndDate) {
		this.sEndDate = sEndDate;
	}
	public String getsDisPart() {
		return this.sDisPart;
	}
	public void setsDisPart(String sDisPart) {
		this.sDisPart = sDisPart;
	}
	public static void main(String[] args)  {
//		hashTest();
        PartitionByDateExDis partitionByDate = new PartitionByDateExDis();
		
		partitionByDate.setDateFormat("yyyy-MM-dd HH:mm:ss");
		partitionByDate.setsBeginDate("2019-01-1 00:00:00");
		partitionByDate.setsEndDate("2019-12-31 23:59:59");
		partitionByDate.setsPartionDay("1");
		partitionByDate.setsDisPart("3");
		partitionByDate.init();
		int r = partitionByDate.calculate("2019-11-1 09:00:00");
		System.out.println(r);
		SimpleDateFormat format = new SimpleDateFormat(partitionByDate.dateFormat);
		try {
			Date ss = format.parse("2019-01-1 00:00:00");
			
			Calendar c = Calendar.getInstance();
			c.setTime(ss);
			Date tomorrow = c.getTime();
			r = partitionByDate.calculate(format.format(tomorrow));
			System.out.println(r);
			for (int i = 0; i < 365; i++) {
				c.add(Calendar.DAY_OF_MONTH, 1);
				 tomorrow = c.getTime();
				 r = partitionByDate.calculate(format.format(tomorrow));
				 System.out.println(r);
				 c.setTime(tomorrow);
			}
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
