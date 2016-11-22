package microservice.javaapp.histclientsdataetl;

import java.util.ArrayList;
import java.util.List;

import org.bson.Document;
import org.bson.conversions.Bson;

import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

public class MongoDBConnector
{
	private String						mongodbUserName;
	private String						mongodbPassWord;
	private String						mongodbDBName;
	private String						mongodbServerAddr;
	private int							mongodbServerport;
	private String						mongodbCollection;
	private MongoClient					mongoClient;
	private MongoCollection<Document>	collection;

	public MongoDBConnector(String mongodbUserName, String mongodbPassWord, String mongodbDBName,
			String mongodbServerAddr, int mongodbServerport, String mongodbCollection)
	{
		this.mongodbUserName = mongodbUserName;
		this.mongodbPassWord = mongodbPassWord;
		this.mongodbDBName = mongodbDBName;
		this.mongodbServerAddr = mongodbServerAddr;
		this.mongodbServerport = mongodbServerport;
		this.mongodbCollection = mongodbCollection;
	}

	public void connect()
	{
		ServerAddress serverAddress = new ServerAddress(this.mongodbServerAddr, this.mongodbServerport);
		List<ServerAddress> addrs = new ArrayList<ServerAddress>();
		addrs.add(serverAddress);

		MongoCredential credential = MongoCredential.createScramSha1Credential(this.mongodbUserName, this.mongodbDBName,
				this.mongodbPassWord.toCharArray());

		List<MongoCredential> credentials = new ArrayList<MongoCredential>();
		credentials.add(credential);

		MongoClient mongoClient = new MongoClient(addrs, credentials);
		this.mongoClient = mongoClient;

		MongoDatabase mongoDatabase = mongoClient.getDatabase(this.mongodbDBName)
				.withReadPreference(ReadPreference.secondary());
		MongoCollection<Document> collection = mongoDatabase.getCollection(this.mongodbCollection);
		this.collection = collection;
		return;
	}

	public MongoCursor<Document> find(Bson filter)
	{
		FindIterable<Document> findIterable = this.collection.find(filter);// .batchSize(100000);
		MongoCursor<Document> mongoCursor = findIterable.iterator();

		return mongoCursor;
	}

	public void close()
	{
		this.mongoClient.close();
		return;
	}
}
