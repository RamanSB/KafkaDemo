package producer;


import constants.IKafkaConstants;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerCreator {

    public static Producer<Long, String> createProducer(){
        Properties props = new Properties(); //Properties object similar to a Map of generic-types <K,V> = <Object, Object> - Simply holds properties, aka props.
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, IKafkaConstants.KAFKA_BROKERS); //Bootstrap server is the 1st server accessed in the Kafka Cluster
        props.put(ProducerConfig.CLIENT_ID_CONFIG, IKafkaConstants.CLIENT_ID); //ToDo: Understand where client id value comes from (I think it's a Kafka default value used)

        //Key serializer - kafka uses partition keys, values & timestamps - I believe this simply serializes (saves the keys in a format kind to memory)
        //If our Producer<Long, String> we can see that our key is of type Long & our value is of type String, hence the use of LongSerializer.class value being used for the key serializer class key
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return new KafkaProducer<>(props); //Passing the properties object to create a KafkaProducer (See README.md to understand purpose of Kafka Producer) //ToDo: add notes to README.md
    }
}
