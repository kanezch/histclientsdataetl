package microservice.javaapp.histclientsdataetl;

import java.io.File;
import java.util.Iterator;

import org.apache.log4j.Logger;
import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

public class XMLReader
{
	private static Logger	logger			= Logger.getLogger(XMLReader.class);

	private static String	configfilename	= "./src/main/config/config.xml";

	public static void getconfig(String args0)
	{
		if (args0.equals("production"))
		{
			configfilename = "./src/main/config/config.xml";
		} else
		{
			configfilename = "./src/main/config/config_local.xml";
		}

		try
		{
			File f = new File(configfilename);
			if (!f.exists())
			{
				logger.error("Config file doesn't exist!");
				return;
			}

			SAXReader reader = new SAXReader();
			Document doc;
			doc = reader.read(f);
			Element root = doc.getRootElement();
			Element data;
			Iterator<?> itr = root.elementIterator("VALUE");
			data = (Element) itr.next();

			Config.mongodbUserName = data.elementText("mongodbUserName").trim();
			Config.mongodbPassWord = data.elementText("mongodbPassWord").trim();
			Config.mongodbDBName = data.elementText("mongodbDBName").trim();
			Config.mongodbServerAddr = data.elementText("mongodbServerAddr").trim();
			Config.mongodbServerPort = Integer.parseInt(data.elementText("mongodbServerPort").trim());
			Config.mongodbCollection = data.elementText("mongodbCollection").trim();
			Config.hbaseTableName = data.elementText("hbaseTableName").trim();
			Config.remoteHiveClusterIP = data.elementText("remoteHiveClusterIP").trim();
			Config.remoteHiveClusterUserName = data.elementText("remoteHiveClusterUserName").trim();
			Config.remoteHiveClusterPassword = data.elementText("remoteHiveClusterPassword").trim();
			Config.execCMD = data.elementText("execCMD").trim();
			Config.writePosTableName = data.elementText("writePosTableName").trim();

			StringBuilder builder = new StringBuilder();
			builder.append(" mongodbUserName: ");
			builder.append(Config.mongodbUserName);
			builder.append("\n mongodbPassWord: ");
			builder.append(Config.mongodbPassWord);
			builder.append("\n mongodbDBName: ");
			builder.append(Config.mongodbDBName);
			builder.append("\n mongodbServerAddr: ");
			builder.append(Config.mongodbServerAddr);
			builder.append("\n mongodbServerPort: ");
			builder.append(Config.mongodbServerPort);
			builder.append("\n mongodbCollection: ");
			builder.append(Config.mongodbCollection);
			builder.append("\n hbaseTableName: ");
			builder.append(Config.hbaseTableName);
			builder.append("\n remoteHiveClusterIP: ");
			builder.append(Config.remoteHiveClusterIP);
			builder.append("\n remoteHiveClusterPassword: ");
			builder.append(Config.remoteHiveClusterPassword);
			builder.append("\n remoteHiveClusterUserName: ");
			builder.append(Config.remoteHiveClusterUserName);
			builder.append("\n execCMD: ");
			builder.append(Config.execCMD);
			builder.append("\n writePosTableName: ");
			builder.append(Config.writePosTableName);
			builder.append("\n");
			logger.info(builder.toString());
		} catch (Exception ex)
		{
			logger.error("Error : " + ex.toString());
		}

		return;
	}
}
