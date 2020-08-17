package Broker;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Objects;

public class Topic {
    private final String name;

    private final File topicFile;
    private final TopicWriter topicWriter;
    private final HashMap<String, TopicReader> topicReaders;

    Topic(String name, int numberOfProducers) throws FileNotFoundException {
        this.name = name;
        topicFile = new File("topics" + File.separator + name + ".dat");
        if (topicFile.exists()) {
            if (topicFile.delete()) System.out.println(topicFile.getPath() + " deleted");
        } else if (topicFile.getParentFile() != null && topicFile.getParentFile().mkdirs())
            System.out.println(topicFile.getPath() + " mkdirs");
        topicWriter = new TopicWriter(this, numberOfProducers);
        topicReaders = new HashMap<>();
    }

    public File getTopicFile() {
        return topicFile;
    }

    void addConsumerGroup(String consumerGroupName, int numberOfConsumers) {
        topicReaders.put(consumerGroupName, new TopicReader(this, numberOfConsumers));
    }

    int getMessage(String consumerGroupName, String consumerName) {
        return topicReaders.get(consumerGroupName).getMessage(consumerName);
    }

    void put(String producerName, int value) {
        topicWriter.put(producerName, value);
    }

    void notifyReaders() {
        for (TopicReader topicreader : topicReaders.values()) {
            topicreader.notifyReader();
        }
    }

    void endTopic() {
        for (TopicReader topicreader : topicReaders.values()) {
            topicreader.end();
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Topic topic = (Topic) o;
        return Objects.equals(name, topic.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }

    public String getName() {
        return name;
    }
}
