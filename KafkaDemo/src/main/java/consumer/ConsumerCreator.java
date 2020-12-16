package consumer;

import constants.IKafkaConstants;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.Properties;

import static constants.IKafkaConstants.OFFSET_RESET_EARLIER;
import static constants.IKafkaConstants.TOPIC_NAME;

public class ConsumerCreator {
    //I believe it makes logical sense for the types of the consumer to match the types of the producer
    // as what is produced is what is consumed

    public static Consumer<Long, String> createConsumer(){
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, IKafkaConstants.KAFKA_BROKERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, IKafkaConstants.GROUP_ID_CONFIG); //Used to identify what group this consumer belongs to
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        //controls the maximum number of records returned in a single call to poll() (default is 500) i.e. max no of records read in 1 iteration
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, IKafkaConstants.MAX_POLL_RECORDS);
        /*see below for offset management
              https://jaceklaskowski.gitbooks.io/apache-kafka/content/kafka-properties-enable-auto-commit.html
              https://docs.confluent.io/platform/current/clients/consumer.html#:~:text=Second%2C%20use%20auto.,%E2%80%9D%20offset%20(the%20default).
         */
        /*  To my understanding: when a consumer is reading messages from a partition in kafka - it has an offset value which is used
            to determine the index of the next message to read. We can manually control when the offset value is committed, by setting the above
            property to false and invoking [Consumer].commit() manually.    */
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); //turning off periodic commits of offset - allowing manual control
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OFFSET_RESET_EARLIER);

        Consumer<Long, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(TOPIC_NAME));//here we will ensure our consumer is subscribed to a kafka topic
        return consumer;
    }
}
