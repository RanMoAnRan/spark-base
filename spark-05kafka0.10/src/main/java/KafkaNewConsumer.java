import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.*;

public class KafkaNewConsumer {

    public static void main(String[] args) {

        // a. Kafka Consumer相关配置参数
        Properties props = new Properties();
        props.put("bootstrap.servers", "hadoop01:9092,hadoop02:9092,hadoop03:9092");
        props.put("group.id", "test");
        props.put("enable.auto.commit", "false");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        // Message中Key和Value的使用反序列化类
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        // 构建KafkaConsumer用于获取数据
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // 消费哪些Topic数据
        consumer.subscribe(Arrays.asList("orderTopic"));


        while (true) {
            // 每次拉取100条数据
            ConsumerRecords<String, String> records = consumer.poll(100);

            Iterator<ConsumerRecord<String, String>> iterator = records.iterator();
            while (iterator.hasNext()) {
                ConsumerRecord<String, String> record = iterator.next();
                System.out.println("topic = " + record.topic() + ", partition = " + record.partition()
                        + ", offset = " + record.offset() + ", key = " + record.key() + ", value = " + record.value());
            }
        }

    }
}
