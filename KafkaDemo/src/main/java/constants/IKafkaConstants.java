package constants;


public interface IKafkaConstants {

    //The hostname:port of the kafka server that serves as a broker we want to connect to - values are found in the config/server.properties file
    //If we are running kafka in clusters we will comma separate all brokers localhost:port pairing.
    public static String KAFKA_BROKERS = "localhost:9091";

    public static Integer MESSAGE_COUNT = 1000;
    public static String CLIENT_ID = "client1";
    public static String TOPIC_NAME = "demo";
    public static String GROUP_ID_CONFIG = "consumerGroup1";
    public static Integer MAX_NO_MESSAGE_FOUND_COUNT = 100;
    public static String OFFSET_RESET_LATEST = "latest";
    public static String OFFSET_RESET_EARLIER = "earliest";
    public static Integer MAX_POLL_RECORDS = 1;

}