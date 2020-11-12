import Kafka.KafkaProducer;
import Utils.UserBehaviorCsvFileReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.stream.Stream;

public class SendMessageApplication {
    public static void main(String[] args) throws Exception {
        // 文件地址
        String filePath = "hdfs://mycluster/test/UserBehavior.csv";
        // kafka topic
        String topic = "user_behavior";
        // kafka borker地址
        String broker = "172.19.241.121:9092";

        Stream.generate(new UserBehaviorCsvFileReader(filePath))
                .sequential()
                .forEachOrdered(new KafkaProducer(topic, broker));
    }
}
