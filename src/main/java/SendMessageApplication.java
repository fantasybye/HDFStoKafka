import Kafka.KafkaProducer;
import Utils.UserBehaviorCsvFileReader;
import Utils.WeiboContentCsvFileReader;
import java.util.stream.Stream;

public class SendMessageApplication {
    public static void main(String[] args) throws Exception {
        // 文件地址
        String filePath = "hdfs://mycluster/weibo/content.csv";
//        String filePath = "hdfs://mycluster/test/UserBehavior.csv";

        // kafka topic
        String topic = "weibo_content";
//        String topic = "user_behavior";
        // kafka borker地址
        String broker = "172.19.241.121:9092,172.19.241.133:9092,172.19.241.133:9092";

//        Stream.generate(new UserBehaviorCsvFileReader(filePath))
        Stream.generate(new WeiboContentCsvFileReader(filePath))
                .sequential()
                .forEachOrdered(new KafkaProducer(topic, broker));
    }
}
