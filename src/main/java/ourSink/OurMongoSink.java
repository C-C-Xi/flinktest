package ourSink;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.internal.MongoClientImpl;
import entity.LogChangeGold;
import lombok.Data;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.bson.Document;

@Data
public class OurMongoSink extends RichSinkFunction<LogChangeGold> {

    private MongoClient client;
    private MongoDatabase db;
    private MongoCollection<Document> collection;

    @Override
    public void open(Configuration parameters) throws Exception {

    }

    @Override
    public void invoke(LogChangeGold value, Context context) throws Exception {

    }

    @Override
    public void close() throws Exception {
    }
}
