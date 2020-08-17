package Broker;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.logging.Level;
import java.util.logging.Logger;

public class TopicReader {

    private final RandomAccessFile topicFile;
    private final MySemaphore semaphore;
    private volatile String consumerName;
    private final int numberOfConsumers;
    private volatile boolean ended;

    TopicReader(Topic topic, int numberOfConsumers) {
        try {
            this.topicFile = new RandomAccessFile(topic.getTopicFile(), "r");
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
        this.semaphore = new MySemaphore();
        this.consumerName = null;
        this.numberOfConsumers = numberOfConsumers;
        this.ended = false;
    }

    public int getMessage(String consumerName) {
        if (!consumerName.equals(this.consumerName)) {
            semaphore.acquire();
        }
        if (ended && !hasNext()) {
            semaphore.release();
            Logger.getLogger(Logger.GLOBAL_LOGGER_NAME).log(Level.INFO,
                    "consumer finish, consumer name:"+consumerName);
            return -10;
        }
        String value = readLine();
        if ("start".equals(value)) {
            Logger.getLogger(Logger.GLOBAL_LOGGER_NAME).log(Level.INFO,
                    "consumer:"+consumerName+" start getting a transaction");
            this.consumerName = consumerName;
            value = readLine();
        }
        if (this.consumerName != null) {
            if (!"end".equals(readNextLine())) {
                return Integer.parseInt(value);
            }
            readLine();
            Logger.getLogger(Logger.GLOBAL_LOGGER_NAME).log(Level.INFO,
                    "consumer:"+consumerName+" end the transaction");
            this.consumerName = null;
        }else Logger.getLogger(Logger.GLOBAL_LOGGER_NAME).log(Level.INFO,
                "consumer:"+consumerName+" get a message:"+value);
        semaphore.release();
        return Integer.parseInt(value);
    }


    private String readLine() {
        try {
            return this.topicFile.readUTF();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    private String readNextLine() {
        try {
            long pointer = this.topicFile.getFilePointer();
            String value = this.topicFile.readUTF();
            this.topicFile.seek(pointer);
            return value;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private boolean hasNext() {
        try {
            return topicFile.getFilePointer() < topicFile.length();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    void notifyReader() {
        semaphore.addSignal(1);
    }

    void end() {
        ended = true;
        semaphore.addSignal(numberOfConsumers);
    }
}
