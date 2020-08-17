package main;

import Broker.MessageBroker;
import Consumer.ConsumerGroup;
import Producer.ProducerGroup;

import java.io.File;

public class Program {

    private final String[] args;
    private final MessageBroker messageBroker;

    Program(String[] args) {
        this.args = args;
        messageBroker = new MessageBroker();
    }

    private File getProducerGroupDirectory() {
        File producerDirectory = new File("data" + File.separator);
        if (args.length > 0) {
            producerDirectory = new File(args[0]);
        }

        return producerDirectory;
    }

    void run() {
        File producerGroupDirectory = getProducerGroupDirectory();
        File producerGroupDirectory1 = new File("data2/");
        String topicName = producerGroupDirectory.getName();
        String topicName1 = producerGroupDirectory1.getName();

        String consumerGroupName = topicName + "Readers1";
        String consumerGroupName1 = topicName + "Readers2";
        int numberOfConsumers = 10;
        int numberOfConsumers1 = 7;

        String consumerGroupName2 = topicName1 + "Readers1";
        String consumerGroupName3 = topicName1 + "Readers2";
        int numberOfConsumers2 = 10;
        int numberOfConsumers3 = 7;

        ProducerGroup producerGroup = new ProducerGroup(messageBroker, producerGroupDirectory, topicName);
        ProducerGroup producerGroup1 = new ProducerGroup(messageBroker,producerGroupDirectory1,topicName1);
        ConsumerGroup consumerGroup = new ConsumerGroup(messageBroker, topicName, consumerGroupName,
                consumerGroupName + ".txt", numberOfConsumers);
        ConsumerGroup consumerGroup1 = new ConsumerGroup(messageBroker, topicName, consumerGroupName1,
                consumerGroupName1 + ".txt", numberOfConsumers1);

        ConsumerGroup consumerGroup2 = new ConsumerGroup(messageBroker, topicName1, consumerGroupName2,
                consumerGroupName2 + ".txt", numberOfConsumers2);
        ConsumerGroup consumerGroup3 = new ConsumerGroup(messageBroker, topicName1, consumerGroupName3,
                consumerGroupName3 + ".txt", numberOfConsumers3);

        producerGroup.start();
        producerGroup1.start();


        consumerGroup.start();
        consumerGroup1.start();

        consumerGroup2.start();
        consumerGroup3.start();


        try {
            producerGroup.join();
            consumerGroup.join();
            consumerGroup1.join();

            producerGroup1.join();
            consumerGroup2.join();
            consumerGroup3.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }
}

