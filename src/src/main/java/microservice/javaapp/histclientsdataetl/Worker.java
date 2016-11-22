package microservice.javaapp.histclientsdataetl;

import static com.mongodb.client.model.Filters.gte;
import static com.mongodb.client.model.Filters.lt;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import org.apache.log4j.Logger;
import org.bson.Document;
import org.bson.conversions.Bson;

import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Filters;

public class Worker
{
	private Logger				logger;
	private MongoDBConnector	mongoDBConnector;
	private HbaseConnector		hbaseConnector;

	public Worker()
	{
		this.logger = Logger.getLogger(Worker.class);
	}

	public void connect() throws IOException
	{
		/* connect to mongodb */
		this.mongoDBConnector = new MongoDBConnector(Config.mongodbUserName, Config.mongodbPassWord,
				Config.mongodbDBName, Config.mongodbServerAddr, Config.mongodbServerPort, Config.mongodbCollection);
		this.mongoDBConnector.connect();

		/* connect to hbase */
		this.hbaseConnector = new HbaseConnector(Config.hbaseTableName, Config.writePosTableName);

		this.hbaseConnector.connect();

		return;
	}

	public void disconnect()
	{
		this.mongoDBConnector.close();
		try
		{
			this.hbaseConnector.close();
		} catch (IOException e)
		{
			logger.error("@ETL@" + e.getClass().getName() + ": " + e.getMessage());
		}
		return;
	}

	public void working() throws IOException
	{
		int readCount;
		int startFindDayIndex = 0;
		int endFindDayIndex = 0;
		Date utcFindStartDate = null;
		Date utcCurrWriteDatePos = null;
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		sdf.setTimeZone(TimeZone.getTimeZone("GMT+8"));

		utcFindStartDate = this.getCurrentPosFromDB();
		utcCurrWriteDatePos = utcFindStartDate;
		startFindDayIndex = (int) ((utcFindStartDate.getTime() + TimeZone.getDefault().getRawOffset())
				/ (24 * 60 * 60 * 1000));
		endFindDayIndex = (int) ((new Date().getTime() + TimeZone.getDefault().getRawOffset()) / (24 * 60 * 60 * 1000));

		logger.info(
				"@ETL@ Start finding at time: " + sdf.format(new Date()) + ", startFindDayIndex=" + startFindDayIndex);

		Bson queryFilter = Filters.and(gte("upLineDay", startFindDayIndex), lt("upLineDay", endFindDayIndex));
		MongoCursor<Document> mongoCursor = this.mongoDBConnector.find(queryFilter);

		logger.info("@ETL@ Finding finished at time: " + sdf.format(new Date()));

		readCount = 0;
		Date upLineDate;
		while (mongoCursor.hasNext())
		{
			Document doc = mongoCursor.next();
			upLineDate = doc.getDate("upLineDate");
			if (null != upLineDate)
			{
				if (utcFindStartDate.before(upLineDate))
				{
					this.hbaseConnector.put(doc);
				}

				if (utcCurrWriteDatePos.before(upLineDate))
				{
					utcCurrWriteDatePos = upLineDate;
				}
			}

			readCount++;
		}

		logger.info("@ETL@ Wrting to Hbase has been finished !");

		if (false == utcFindStartDate.equals(utcCurrWriteDatePos))
		{
			this.setCurrentPosToDB(utcCurrWriteDatePos);
		}

		StringBuilder builder = new StringBuilder();
		builder.append("readCount: ");
		builder.append(readCount);
		builder.append(",");
		builder.append("putCount: ");
		builder.append(this.hbaseConnector.putCount);
		builder.append(",");
		builder.append("time: ");
		builder.append(sdf.format(new Date()));
		logger.info("@ETL@" + builder.toString());

		this.hbaseConnector.flush();

		Util.changeLogFileName();
		logger.info("@ETL@ Change logfile name has finished: " + sdf.format(new Date()));

		RemoteExecuteCommand rec = new RemoteExecuteCommand();
		String result = rec.execute(Config.execCMD);
		logger.info("@ETL@ new Executing remote hive shell has finished: " + sdf.format(new Date()));
		logger.info("@ETL@ result:" + result);
	}

	public void setCurrentPosToDB(Date utcFindStartDate)
	{
		this.hbaseConnector.upDateLatestOnlineTime(utcFindStartDate);
		return;
	}

	public Date getCurrentPosFromDB()
	{
		Date latestOnlineDate = null;
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		sdf.setTimeZone(TimeZone.getTimeZone("GMT+8"));

		try
		{
			latestOnlineDate = this.hbaseConnector.getLatestOnlineTime();
			if (null == latestOnlineDate)
			{
				latestOnlineDate = Util.getUTCZero();
			}
		} catch (IOException e)
		{
			logger.error("@ETL@" + e.getClass().getName() + ": " + e.getMessage());
			latestOnlineDate = Util.getUTCZero();
		}

		logger.info("@ETL@ getCurrentPosFromDB: " + sdf.format(latestOnlineDate));
		return latestOnlineDate;
	}
}
