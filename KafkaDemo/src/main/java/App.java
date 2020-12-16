import constants.IKafkaConstants;
import consumer.ConsumerCreator;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import producer.ProducerCreator;

import java.time.Duration;
import java.util.concurrent.ExecutionException;

/**
 * Here we will demonstrate how to use the Producer & Consumer entities to publish
 * and receive messages via Kafka.
 */
public class App {

    public static void main(String[] args){
        //runProducer();
        runConsumer();
    }

    static void runConsumer(){
        try(Consumer<Long, String> consumer = ConsumerCreator.createConsumer()) {
            int noMessagesFound = 0; //Keep tracking of the number of times we DO NOT find a message on the kafka topic to consume.

            while (true) {
                //polling for records published by the producer will occur every 1s - once consumed, they will be ConsumerRecords
                ConsumerRecords<Long, String> consumerRecords = consumer.poll(Duration.ofNanos(100000));
                if (consumerRecords.count() == 0) {
                    noMessagesFound++;
                    if (noMessagesFound > IKafkaConstants.MAX_NO_MESSAGE_FOUND_COUNT) {
                        //if we don't find messages 100 times then we stop consuming.
                        break;
                    } else {
                        continue;
                    }
                }
                //print individual ConsumerRecord
                consumerRecords.forEach(record -> {
                    /*System.out.println("Record Key " + record.key());
                    System.out.println("Record value " + record.value());
                    System.out.println("Record partition " + record.partition());
                    System.out.println("Record offset " + record.offset());
                    System.out.println("---");*/
                    System.out.format("%s %d %d \n", record.value(), record.partition(), record.offset());
                });
                //this commits the offset value - which is used to denote the position of the next read message in a partition
                consumer.commitAsync();
            }
        } catch(Exception ex){
            ex.printStackTrace();
        }
    }

    static void runProducer(){
        try(Producer<Long, String> producer = ProducerCreator.createProducer()){

            for(int index = 0; index < IKafkaConstants.MESSAGE_COUNT; index++) {
                ProducerRecord<Long, String> record = new ProducerRecord<Long, String>(
                        IKafkaConstants.TOPIC_NAME, "This is record " + index
                );

                RecordMetadata metadata = producer.send(record).get(); //producer.send(record) publishes the record to the kafka topic declared when instantiating the ProducerRecord
                //System.out.format("Record sent with key %d to partition %d with offset %d", index, metadata.partition(), metadata.offset());
                System.out.format("%d %d %d\n", index, metadata.partition(), metadata.offset());
            }
        } catch(ExecutionException | InterruptedException e){
            System.out.println("Error in sending record");
            System.out.println(e);
        }
    }
}
