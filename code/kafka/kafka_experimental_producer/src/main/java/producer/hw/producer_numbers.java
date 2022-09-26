package producer.hw;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;

public class producer_numbers {

    static long count_values = 0;

    public static void main(String[] args) throws InterruptedException {
        String bootstrap_server = "127.0.0.1:9092";
        if (args.length > 0) {
            bootstrap_server = args[0];
            System.out.println("BootsTrap Server: " + bootstrap_server);
        }

        System.out.println(bootstrap_server);
        // starting Kafka and other configurations----------------------------------------------------
        final Logger logger = LoggerFactory.getLogger(producer.class);

        // KAFKA CONFIGURATIONS
        String bootstrapServers = "localhost:9092";// zookeepper server
        String topic = "stream-topic"; // topic
        // create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap_server);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        final int ADMIN_CLIENT_TIMEOUT_MS = 2000;
        boolean kafka_up = false;
        while(!kafka_up) {
            try (AdminClient client = AdminClient.create(properties)) {
                client.listTopics(new ListTopicsOptions().timeoutMs(ADMIN_CLIENT_TIMEOUT_MS)).listings().get();
                kafka_up = true;
            } catch (ExecutionException ex) {
                System.out.println("Kafka is not available, timed out after {} ms " + ADMIN_CLIENT_TIMEOUT_MS);
                kafka_up = false;
                Thread.sleep(ADMIN_CLIENT_TIMEOUT_MS);
            }
        }


        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        System.out.println("STATUS: Kafka server up, producer starting...");
        Random random_generator = new Random(19700621);

        // stops when initial data is before then final data or iterator is bigger then size lines
        while (true) {



            Integer random_number = random_generator.nextInt(100) + 1;

            count_values++;
            if (count_values%1000000 == 0){
            SimpleDateFormat formatter= new SimpleDateFormat("HH:mm:ss");
            Date date = new Date(System.currentTimeMillis());
            System.out.println(formatter.format(date)+" "+String.valueOf(count_values)+" "+random_number);
            }
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, random_number.toString());

            // send data - asynchronous
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    // executes every time a record is successfully sent or an exception is thrown
                    if (e == null) {
                        // the record was successfully sent
                        /*logger.info("Received new metadata. \n" +
                                    "Topic:" + recordMetadata.topic() + "\n" +
                                    "Partition: " + recordMetadata.partition() + "\n" +
                                    "Offset: " + recordMetadata.offset() + "\n" +
                                    "Timestamp: " + recordMetadata.timestamp());

                             */
                    } else {
                        logger.error("Error while producing", e);
                    }
                }
            });
            //logger.info(recorder_lines.get(j));
        }
        //-------------------------------------------------------------------------------------------

        // flush data
        //producer.flush();
        // flush and close producer
        //producer.close();
    }
}
