//package com.bigdata.hive;
//
//import java.text.ParseException;
//import java.text.SimpleDateFormat;
//import java.util.Date;
//import java.util.Locale;
//
//import org.apache.commons.lang.StringUtils;
//import org.apache.hadoop.hive.ql.exec.UDF;
//import org.apache.hadoop.io.Text;
//
//public class HiveUdf extends UDF{
//
//	public Text evaluate(Text time) {
//
//		String output = null;
//		SimpleDateFormat inputDate = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.ENGLISH);
//		SimpleDateFormat outputDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//
//		if (time == null) {
//			return null;
//		}
//
//		if (StringUtils.isBlank(time.toString())) {
//			return null;
//		}
//
//		String parser = time.toString().replaceAll("\"", "");
//
//		try {
//			Date parseDate = inputDate.parse(parser);
//			output = outputDate.format(parseDate);
//		} catch (ParseException e) {
//			e.printStackTrace();
//		}
//		return new Text(output);
//}
//}
