package Consumer;

import Broker.MessageBroker;
import Broker.NoSuchTopicException;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.ArrayList;

public class ConsumerGroup extends Thread {
    private final ArrayList<Consumer> consumers;
    private final MessageBroker messageBroker;
    private final String topicName;
    private final String groupName;
    private final int numberOfConsumers;

    private final File consumerGroupFile;
    private PrintWriter printWriter;

    public ConsumerGroup(MessageBroker messageBroker, String topicName, String groupName, String consumerGroupFileName, int numberOfConsumers) {
        this.messageBroker = messageBroker;
        this.consumerGroupFile = new File("consumerGroups" + File.separator + consumerGroupFileName);
        this.topicName = topicName;
        this.groupName = groupName;
        this.numberOfConsumers = numberOfConsumers;
        this.consumers = new ArrayList<>();
        if (consumerGroupFile.exists()) {
            if (consumerGroupFile.delete()) System.out.println(consumerGroupFile.getPath() + " deleted");
        } else if (consumerGroupFile.getParentFile() != null && consumerGroupFile.getParentFile().mkdirs())
            System.out.println(consumerGroupFile.getPath() + " mkdirs");
        try {
            this.messageBroker.addConsumerGroup(topicName, groupName, numberOfConsumers);
        } catch (NoSuchTopicException e) {
            throw new RuntimeException(e);
        }
    }

    private void initialize() throws FileNotFoundException {
        for (int i = 0; i < numberOfConsumers; i++) {
            String consumerName = groupName + "_" + i;
            consumers.add(new Consumer(this, consumerName));
        }

        printWriter = new PrintWriter(consumerGroupFile);
    }

    public void run() {
        try {
            initialize();

            for (Consumer consumer : consumers) {
                consumer.start();
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    public synchronized void performAction(Consumer consumer, int value) {
        printWriter.println("Consumer with name " + consumer.getConsumerName() + " read the value " + value);
        printWriter.flush();
    }

    public String getGroupName() {
        return groupName;
    }

    public String getTopicName() {
        return topicName;
    }

    public MessageBroker getMessageBroker() {
        return messageBroker;
    }
}

