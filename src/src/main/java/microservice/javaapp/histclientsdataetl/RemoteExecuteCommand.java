package microservice.javaapp.histclientsdataetl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;

import org.apache.log4j.Logger;

import ch.ethz.ssh2.ChannelCondition;
import ch.ethz.ssh2.Connection;
import ch.ethz.ssh2.Session;
import ch.ethz.ssh2.StreamGobbler;

public class RemoteExecuteCommand
{
	// 字符编码默认是utf-8
	private static String	DEFAULTCHART	= "UTF-8";
	private Connection		conn;
	private String			ip;
	private String			userName;
	private String			userPwd;
	private static Logger	logger			= Logger.getLogger(RemoteExecuteCommand.class);

	public RemoteExecuteCommand()
	{
		this.ip = Config.remoteHiveClusterIP;
		this.userName = Config.remoteHiveClusterUserName;
		this.userPwd = Config.remoteHiveClusterPassword;
	}

	public Boolean login()
	{
		boolean flg = false;
		try
		{
			conn = new Connection(ip);
			conn.connect();
			flg = conn.authenticateWithPassword(userName, userPwd);
		} catch (IOException e)
		{
			e.printStackTrace();
		}
		return flg;
	}

	public String execute(String cmd)
	{
		String result = "";
		try
		{
			if (login())
			{
				Session session = conn.openSession();

				logger.info("@ETL@ cmd: " + cmd);

				session.requestPTY("bash");
				session.startShell();
				PrintWriter out = new PrintWriter(session.getStdin());
				out.println(cmd);
				out.flush();
				out.close();

				session.waitForCondition(ChannelCondition.CLOSED | ChannelCondition.EOF | ChannelCondition.EXIT_STATUS,
						60000);

				logger.info("@ETL@: exec has finished!");
				session.close();
				conn.close();
			}
		} catch (IOException e)
		{
			e.printStackTrace();
		}
		return result;
	}

	public void executeSuccess(String cmd)
	{
		try
		{
			if (login())
			{
				Session session = conn.openSession();
				session.execCommand(cmd);
				logger.info("@ETL@ executing cmd!");

				try
				{
					Thread.sleep(120000);
					logger.info("@ETL@ wake up now!");
				} catch (InterruptedException e)
				{
					logger.error("@ETL@" + e.getClass().getName() + ": " + e.getMessage());
				}
				session.close();
				conn.close();
			}
		} catch (IOException e)
		{
			logger.error("@ETL@" + e.getClass().getName() + ": " + e.getMessage());
		}
		return;
	}

	private String processStdout(InputStream in, String charset)
	{
		InputStream stdout = new StreamGobbler(in);
		StringBuffer buffer = new StringBuffer();
		;
		try
		{
			BufferedReader br = new BufferedReader(new InputStreamReader(stdout, charset));
			String line = null;
			while ((line = br.readLine()) != null)
			{
				buffer.append(line + "\n");
			}
		} catch (UnsupportedEncodingException e)
		{
			e.printStackTrace();
		} catch (IOException e)
		{
			e.printStackTrace();
		}
		return buffer.toString();
	}

	public static void setCharset(String charset)
	{
		DEFAULTCHART = charset;
	}

	public Connection getConn()
	{
		return conn;
	}

	public void setConn(Connection conn)
	{
		this.conn = conn;
	}

	public String getIp()
	{
		return ip;
	}

	public void setIp(String ip)
	{
		this.ip = ip;
	}

	public String getUserName()
	{
		return userName;
	}

	public void setUserName(String userName)
	{
		this.userName = userName;
	}

	public String getUserPwd()
	{
		return userPwd;
	}

	public void setUserPwd(String userPwd)
	{
		this.userPwd = userPwd;
	}
}
