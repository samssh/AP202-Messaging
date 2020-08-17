package Producer;

import Broker.MessageBroker;

import java.io.File;
import java.util.ArrayList;

public class ProducerGroup extends Thread {
    private final ArrayList<Producer> producers;
    private final File producerGroupDirectory;
    private final MessageBroker messageBroker;
    private final String topicName;

    @SuppressWarnings("ConstantConditions")
    public ProducerGroup(MessageBroker messageBroker, File producerGroupDirectory, String topicName) {
        this.messageBroker = messageBroker;
        this.producerGroupDirectory = producerGroupDirectory;
        this.topicName = topicName;
        this.producers = new ArrayList<>();
        if (!producerGroupDirectory.exists())
            if (producerGroupDirectory.getParentFile() != null && producerGroupDirectory.getParentFile().mkdirs())
                System.out.println(producerGroupDirectory.getPath() + " mkdirs");
        this.messageBroker.addProducerGroup(topicName, producerGroupDirectory.listFiles().length);
    }

    @SuppressWarnings("ConstantConditions")
    private void initialize() {
        for (File file : producerGroupDirectory.listFiles()) {
            this.producers.add(new Producer(messageBroker, topicName, file.getName(), file));
        }
    }

    public void run() {
        initialize();
        for (Producer producer : producers) {
            producer.start();
        }
    }
}
