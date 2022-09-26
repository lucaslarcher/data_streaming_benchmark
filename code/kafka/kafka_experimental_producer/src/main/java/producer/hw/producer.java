package producer.hw;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class producer {
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
                Thread.sleep(5000);
            }
        }

        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);


        String pathFile = "test.txt"; // input file

        if (args.length > 0) // input parameters
        {
            //pathFile = args[0]; //pathfile
            //System.out.println(pathFile);
        }
        //-------------------------------------------------------------------------------------------

        ArrayList<String> lines = new ArrayList<String>(); // lines
        ArrayList<Integer> msc = new ArrayList<Integer>(); // milliseconds


        //read file to memory -----------------------------------------------------------------------
        try {

            File file = new File(pathFile);//open file
            BufferedReader br = new BufferedReader(new FileReader(file));
            String line;

            Boolean firstLine = true;
            int dataAcress = 0;


            long tm = 0;
            while ((line = br.readLine()) != null) {
                if(line.split(" ").length == 0 || line.length() == 0){
                    continue;
                }
                lines.add(line);
            }
            System.out.println(tm);
        } catch (Exception e) {
            System.out.println("The file cant be found");
        }


        // stops when initial data is before then final data or iterator is bigger then size lines
        int i = 1;
        while (true) {
            i++;

            // create a producer record
            ArrayList<String> recorder_lines = new ArrayList<String>(); // recorder lines to send like a batch because i had only a millisecond as a measure

            /*
            for (int k = 0; k < lines.size(); k++) {
                recorder_lines.add(lines.get((int) k));
            }*/

            recorder_lines.add(lines.get(i % lines.size()));

            //send messages
            for (int j = 0; j < recorder_lines.size(); j++) {
                // create a producer record
                ProducerRecord<String, String> record =
                        new ProducerRecord<String, String>(topic, recorder_lines.get(j));


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

            //Thread.currentThread().sleep(100); // 1 segundo

            /*i++;
            if (i >= 1000000) {
                break;
            }

             */
            i++;
        }
        //-------------------------------------------------------------------------------------------

        // flush data
        //producer.flush();
        // flush and close producer
        //producer.close();
    }
}
