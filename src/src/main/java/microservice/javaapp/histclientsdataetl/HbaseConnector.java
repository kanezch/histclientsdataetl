package microservice.javaapp.histclientsdataetl;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.bson.Document;

public class HbaseConnector
{
	private String			clientsTableName;
	private String			writePosTableName;
	private Configuration	conf;
	// private Table clientsTable;
	private Table			writePosTable;
	private BufferedMutator	mutator;
	private Put				writePosPut;
	private Get				writePosGet;
	private Connection		connection;
	public int				putCount;
	private static Logger	logger	= Logger.getLogger(HbaseConnector.class);

	public HbaseConnector(String clientsTableName, String writePosTableName)
	{
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		sdf.setTimeZone(TimeZone.getTimeZone("GMT+8"));

		this.clientsTableName = clientsTableName;
		this.writePosTableName = writePosTableName;
		this.conf = HBaseConfiguration.create();
		this.putCount = 0;
	}

	public void connect() throws IOException
	{
		this.connection = ConnectionFactory.createConnection(this.conf);
		// this.clientsTable =
		// this.connection.getTable(TableName.valueOf(this.clientsTableName));
		this.writePosTable = this.connection.getTable(TableName.valueOf(this.writePosTableName));

		BufferedMutatorParams mutatorPara = new BufferedMutatorParams(TableName.valueOf(this.clientsTableName));
		mutatorPara.writeBufferSize(5 * 1024 * 1024);

		this.mutator = this.connection.getBufferedMutator(mutatorPara);

		this.writePosPut = new Put(Bytes.toBytes("writePosRowKey"));
		this.writePosGet = new Get(Bytes.toBytes("writePosRowKey"));
		return;
	}

	public void put(Document doc) throws IOException
	{
		Put put = null;
		put = Util.castHbaseRow(doc);
		if (null != put)
		{
			this.mutator.mutate(put);
			this.putCount++;
		}

		return;
	}

	public void close() throws IOException
	{
		this.mutator.close();
		// this.clientsTable.close();
		this.writePosTable.close();
		this.connection.close();
		return;
	}

	public void flush() throws IOException
	{
		this.mutator.flush();
		this.putCount = 0;
		return;
	}

	public void upDateLatestOnlineTime(Date uplineDate)
	{
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		sdf.setTimeZone(TimeZone.getTimeZone("GMT+8"));

		String uplineDateStr = sdf.format(uplineDate);
		this.writePosPut.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("uplineDate"), Bytes.toBytes(uplineDateStr));
		try
		{
			this.writePosTable.put(this.writePosPut);
			logger.info("@ETL@ upDateLatestOnlineTime: " + uplineDateStr);
		} catch (IOException e)
		{
			logger.error("@ETL@" + e.getClass().getName() + ": " + e.getMessage());
		}

		return;
	}

	public Date getLatestOnlineTime() throws IOException
	{
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		sdf.setTimeZone(TimeZone.getTimeZone("GMT+8"));
		Date uplineDate = null;

		Result result = this.writePosTable.get(writePosGet);
		byte[] val = result.getValue(Bytes.toBytes("f1"), Bytes.toBytes("uplineDate"));
		if (null != val)
		{
			String valStr = Bytes.toString(val);
			logger.info("@ETL@ getLatestOnlineTime: " + valStr);

			try
			{
				uplineDate = sdf.parse(valStr);
				logger.info("@ETL@ get uplineDate from hbase: " + sdf.format(uplineDate));
			} catch (ParseException e)
			{
				logger.error("@ETL@" + e.getClass().getName() + ": " + e.getMessage());
			}
		} else
		{
			logger.info("@ETL@ getLatestOnlineTime: val equals null.");
		}

		return uplineDate;
	}
}
