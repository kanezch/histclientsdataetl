package microservice.javaapp.histclientsdataetl;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.TimeZone;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.log4j.Logger;

public class HistClientsDataETL
{
	private static Logger logger = Logger.getLogger(HistClientsDataETL.class);

	public static void main(String[] args) throws IOException, ParseException
	{
		Util.addDNSToHosts(args[0]);

		XMLReader.getconfig(args[0]);

		startTimer();
	}

	public static void startTimer()
	{
		Calendar calendar = Calendar.getInstance();
		calendar.setTimeZone(TimeZone.getTimeZone("GMT+8"));

		// calendar.set(calendar.get(Calendar.YEAR),
		// calendar.get(Calendar.MONTH), calendar.get(Calendar.DATE) + 1, 0, 5,
		// 0);

		calendar.set(calendar.get(Calendar.YEAR), calendar.get(Calendar.MONTH), calendar.get(Calendar.DATE), 10, 20, 0);

		Timer timer = new Timer();
		timer.scheduleAtFixedRate(new TimerTask() {

			public void run()
			{
				int count = 0;
				boolean isOver = false;
				SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
				sdf.setTimeZone(TimeZone.getTimeZone("GMT+8"));

				while ((count < 60) && (false == isOver))
				{
					Worker worker = new Worker();

					try
					{
						worker.connect();
						worker.working();
						isOver = true;
					} catch (IOException e)
					{
						logger.error("@ETL@" + e.getClass().getName() + ": " + e.getMessage());
						count++;
					} finally
					{
						worker.disconnect();
						worker = null;
						System.gc();
					}

					try
					{
						Thread.sleep(60000);
					} catch (InterruptedException e)
					{
						logger.error("@ETL@" + e.getClass().getName() + ": " + e.getMessage());
					}
				}
			}

		}, calendar.getTime(), 1000 * 60 * 60 * 24);// 这里设定将延时每天00:05:00固定执行
	}
}