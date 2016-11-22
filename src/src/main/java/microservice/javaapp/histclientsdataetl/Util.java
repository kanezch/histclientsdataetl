package microservice.javaapp.histclientsdataetl;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.bson.Document;

public class Util
{
	private static Logger	logger	= Logger.getLogger(Util.class);
	private static int		iCount	= 0;
	// private static Date today12PM = getToday12PM();
	// private static Date today12PM = getTodayZeroam();

	// static{
	// today12PM = getToday12PM();
	// }

	public static void copyLogFile(String fileName)
	{
		String srcPath = "/workspace/logs_temp/" + fileName;
		String destPath = "/workspace/logs/" + fileName;

		String cmd = "cp " + srcPath + " " + destPath;
		try
		{
			Process ps = Runtime.getRuntime().exec(new String[] { "sh", "-c", cmd });
		} catch (IOException e)
		{
			logger.error("@ETL@ failed to copy log file ");
		}
	}

	public static void changeLogFileName()
	{
		File file = new File("/workspace/logs_temp");
		if (file.exists() && file.isDirectory())
		{
			File[] childFiles = file.listFiles();
			String path = file.getAbsolutePath();
			String logPrefix = "1.1.1.1-histclientsdataetl";
			String logFormat = "1.1.1.1-histclientsdataetl.xxxx-xx-xx";
			String dataFormat = "xxxx-xx-xx";

			for (File childFile : childFiles)
			{
				if (childFile.isFile())
				{
					String oldName = childFile.getName();
					logger.info("@ETL@ child file name: " + oldName);

					if (oldName.contains(logPrefix) && (oldName.length() == logFormat.length()))
					{
						String logDate = oldName.substring(oldName.length() - dataFormat.length(), oldName.length());
						String newName = logPrefix + "-" + logDate.replaceAll("-", "") + ".log";
						childFile.renameTo(new File(path + "/" + newName));
						copyLogFile(newName);
						logger.info("@ETL@ Succeed to rename file " + oldName + "to " + newName);
					}
				}
			}
		}
		return;
	}

	public static void addDNSToHosts(String args0) throws IOException
	{
		String cmd;

		if (args0.equals("production"))
		{
			cmd = "cat src/main/config/addtohosts.properties >> /etc/hosts";
		} else
		{
			cmd = "cat src/main/config/addtohosts_local.properties >> /etc/hosts";
		}

		Process ps = Runtime.getRuntime().exec(new String[] { "sh", "-c", cmd });
		return;
	}

	/*
	 * public static void updateToday12PM() { SimpleDateFormat sdf = new
	 * SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
	 * sdf.setTimeZone(TimeZone.getTimeZone("GMT+8")); today12PM =
	 * getToday12PM(); logger.info("@ETL@ UpDate today 12PM to: " +
	 * sdf.format(today12PM)); return; }
	 * 
	 * 
	 * public static boolean needExecHiveScript() { SimpleDateFormat sdf = new
	 * SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
	 * sdf.setTimeZone(TimeZone.getTimeZone("GMT+8")); Date currentDate = new
	 * Date();
	 * 
	 * logger.info("@ETL@ needExecHiveScript: currentDate: " +
	 * sdf.format(currentDate) + "  " + "today12PM: " + sdf.format(today12PM));
	 * 
	 * if (currentDate.equals(today12PM) || currentDate.after(today12PM)) {
	 * updateToday12PM(); return true; } return false; }
	 */

	public static Date getTodayZeroam()
	{
		Calendar cal = Calendar.getInstance();
		cal.setTimeZone(TimeZone.getTimeZone("GMT+8"));

		cal.set(cal.get(cal.YEAR), cal.get(cal.MONTH), cal.get(cal.DATE), 0, 0, 0);

		logger.info("@ETL@ getTodayZeroam: " + cal.getTime());

		return cal.getTime();
	}

	private static Date getToday12PM()
	{
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		sdf.setTimeZone(TimeZone.getTimeZone("GMT+8"));

		Calendar cal = Calendar.getInstance();
		cal.setTimeZone(TimeZone.getTimeZone("GMT+8"));

		logger.info("@ETL@ current time: " + cal.getTime());

		cal.set(cal.get(Calendar.YEAR), cal.get(Calendar.MONTH), cal.get(Calendar.DATE), 0, 0, 0);

		logger.info("@ETL@ today zero o'clock: " + cal.getTime());

		cal.set(cal.get(Calendar.YEAR), cal.get(Calendar.MONTH), cal.get(Calendar.DATE) + 1, 0, 0, 0);

		logger.info("@ETL@ today 24 o'clock: " + cal.getTime());

		return cal.getTime();
	}

	public static Date getTimeBeforeToday12pm()
	{
		long current = System.currentTimeMillis();
		long zeroam = current / (1000 * 3600 * 24) * (1000 * 3600 * 24) - TimeZone.getDefault().getRawOffset();
		long twelve = zeroam + 24 * 60 * 60 * 1000 - 1;
		Date utcTimeBeforeToday12pm = new Date(twelve);

		return utcTimeBeforeToday12pm;
	}

	public static Date getUTCZero()
	{
		Date utcZeroDate = new Date(0);

		return utcZeroDate;
	}

	public static void setOutPutToFile() throws IOException
	{
		File f = new File("out.txt");
		f.delete();
		f.createNewFile();
		FileOutputStream fileOutputStream = new FileOutputStream(f);
		PrintStream printStream = new PrintStream(fileOutputStream);
		System.setOut(printStream);
		return;
	}

	private static boolean isNullOrEmptyStr(String aString)
	{
		if ((null == aString) || (aString.length() == 0))
		{
			return true;
		}

		return false;
	}

	public static Put castHbaseRow(Document doc)
	{
		String acSNStr = doc.getString("acSN");
		String apSNStr = doc.getString("apSN");
		String clientMACStr = doc.getString("clientMAC");
		Date upLineDate = doc.getDate("upLineDate");
		Date offLineDate = doc.getDate("offLineDate");
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		sdf.setTimeZone(TimeZone.getTimeZone("GMT+8"));

		if (isNullOrEmptyStr(acSNStr) || isNullOrEmptyStr(apSNStr) || isNullOrEmptyStr(clientMACStr)
				|| (null == upLineDate))
		{
			StringBuilder builderErr = new StringBuilder();
			builderErr.append(acSNStr);
			builderErr.append(apSNStr);
			builderErr.append(clientMACStr);
			builderErr.append(upLineDate);
			logger.error("@ETL@ CastHbaseRow null: " + builderErr.toString());
			return null;
		}

		iCount++;

		StringBuilder builder = new StringBuilder();
		builder.append(acSNStr);
		builder.append(apSNStr);
		builder.append(clientMACStr);
		builder.append(sdf.format(upLineDate));
		builder.append(Integer.toString(iCount));

		String rowkeyStr = acSNStr + apSNStr + clientMACStr + sdf.format(upLineDate);

		Put put = new Put(Bytes.toBytes(rowkeyStr));
		put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("acSN"), Bytes.toBytes(acSNStr));
		put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("apSN"), Bytes.toBytes(apSNStr));
		put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("clientMAC"), Bytes.toBytes(clientMACStr));
		put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("upLineDate"), Bytes.toBytes(sdf.format(upLineDate)));

		String clientModeStr = doc.getString("clientMode");
		if (false == isNullOrEmptyStr(clientModeStr))
		{
			put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("clientMode"), Bytes.toBytes(clientModeStr));
		}

		String clientSSIDStr = doc.getString("clientSSID");
		if (false == isNullOrEmptyStr(clientSSIDStr))
		{
			put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("clientSSID"), Bytes.toBytes(clientSSIDStr));
		}

		String clientVendorStr = doc.getString("clientVendor");
		if (false == isNullOrEmptyStr(clientVendorStr))
		{
			put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("clientVendor"), Bytes.toBytes(clientVendorStr));
		}

		String clientNameStr = doc.getString("clientName");
		if (false == isNullOrEmptyStr(clientNameStr))
		{
			put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("clientName"), Bytes.toBytes(clientNameStr));
		}

		String clientIPStr = doc.getString("clientIP");
		if (false == isNullOrEmptyStr(clientIPStr))
		{
			put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("clientIP"), Bytes.toBytes(clientIPStr));
		}

		String apNameStr = doc.getString("ApName");
		if (false == isNullOrEmptyStr(apNameStr))
		{
			put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("ApName"), Bytes.toBytes(apNameStr));
		}

		Boolean alreadyOffLine = doc.getBoolean("alreadyOffLine");
		if (null != alreadyOffLine)
		{
			put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("alreadyOffLine"), Bytes.toBytes(alreadyOffLine));
		}

		Boolean update = doc.getBoolean("update");
		if (null != update)
		{
			put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("update"), Bytes.toBytes(update));
		}

		Integer onlineTime = doc.getInteger("onlineTime");
		if (null != onlineTime)
		{
			put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("onlineTime"), Bytes.toBytes(onlineTime.intValue()));
		}

		if (null != offLineDate)
		{
			put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("offLineDate"), Bytes.toBytes(sdf.format(offLineDate)));
		}

		return put;
	}
}
