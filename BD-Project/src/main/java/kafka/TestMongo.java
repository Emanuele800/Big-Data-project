package kafka;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class TestMongo {
	
	public static void main(String... args) throws Exception {
		MongoClient client = new MongoClient("localhost", 27017);
		MongoDatabase db = client.getDatabase("github_events");
		MongoCollection<Document> eventsCollection = db.getCollection("events");
		Document eventDocument = new Document("_id", Math.random()*10000)
				.append("value", "test");
		eventsCollection.insertOne(eventDocument);
	}
}
