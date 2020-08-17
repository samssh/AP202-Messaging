package Broker;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;
import java.util.logging.*;
import java.util.logging.Formatter;

public class MessageBroker {
    private final Map<String, Topic> topics = new HashMap<>();

    public MessageBroker() {
        Logger logger = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
        File log = new File("log");
        if (!log.exists())
            if (log.mkdirs())
                System.out.println("log directory created");
        try {
            FileHandler fileHandler = new FileHandler("log" + File.separator + "broker.log");
            fileHandler.setFormatter(new MyFormatter());
            logger.addHandler(fileHandler);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            Logger rootLogger = Logger.getLogger("");
            Handler[] handlers = rootLogger.getHandlers();
            if (handlers[0] instanceof ConsoleHandler) {
                rootLogger.removeHandler(handlers[0]);
            }
        }
        logger.setLevel(Level.INFO);
        logger.log(Level.INFO, "message broker creted");

    }

    private void addTopic(String name, int numberOfProducers) {
        try {
            this.topics.put(name, new Topic(name, numberOfProducers));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    public void addProducerGroup(String topicName, int numberOfProducers) {
        if (!topics.containsKey(topicName)) {
            this.addTopic(topicName, numberOfProducers);
            Logger.getLogger(Logger.GLOBAL_LOGGER_NAME).log(Level.INFO, "producer group created, topic name:" + topicName);
        }
    }

    public void addConsumerGroup(String topic, String groupName, int numberOfConsumers)
            throws NoSuchTopicException {
        if (!topics.containsKey(topic))
            throw new NoSuchTopicException(topic);
        topics.get(topic).addConsumerGroup(groupName, numberOfConsumers);
        Logger.getLogger(Logger.GLOBAL_LOGGER_NAME).log(Level.INFO,
                "consumer group:" + groupName + " created, topic name:" + topic);
    }

    public void put(String topic, String producerName, int value) {
        topics.get(topic).put(producerName, value);
    }

    public int getMessage(String topic, String groupName, String consumerName) throws NoSuchTopicException {
        if (!topics.containsKey(topic))
            throw new NoSuchTopicException(topic);

        return topics.get(topic).getMessage(groupName, consumerName);
    }

    private static class MyFormatter extends Formatter {

        @Override
        public String format(LogRecord record) {
            return String.format("%s %s %s \n",
                    new Date(record.getMillis()),
                    record.getLevel().getName(),
                    record.getMessage());
        }

    }

    private static class MyHandler extends Handler{

        @Override
        public void publish(LogRecord record) {
            String s=getFormatter().format(record);
        }

        @Override
        public void flush() {

        }

        @Override
        public void close() throws SecurityException {

        }
    }
}
