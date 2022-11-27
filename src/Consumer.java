import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Arrays;
import java.util.Properties;

public class Consumer {
    public static void main(String[] args) {
        KafkaConsumer consumer;
        String broker = "localhost:9092";
        String topic = "natureNumber";
        Properties prop = new Properties();
        prop.put("bootstrap.servers", broker);
        prop.put("group.id", "test.group");
        prop.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        prop.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new KafkaConsumer(prop);
        consumer.subscribe(Arrays.asList(topic));

        while(true){
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record.value());
                String wor = record.value();
                int coun = wor.length();
                try{
                    Class.forName("com.mysql.jdbc.Driver");
                    Connection con = DriverManager.getConnection("jdbc:mysql://localhost:3306/kafka_db", "root", "");
                    String sql = "INSERT INTO `count_let`(`Message`, `count`) VALUES(?,?)";
                    PreparedStatement stmt = con.prepareStatement(sql);
                    stmt.setString(1,wor);
                    stmt.setInt(2,coun);
                    stmt.executeUpdate();
                }
                catch (Exception e){
                    System.out.println(e);
                }
            }
        }

    }
}