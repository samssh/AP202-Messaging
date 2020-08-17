package Broker;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

public class TopicWriter {
    private final RandomAccessFile randomAccessFile;
    private final Topic topic;
    private final HashMap<String, Transaction> transactions;
    private int numberOfProducers;

    TopicWriter(Topic topic, int numberOfProducers) throws FileNotFoundException {
        this.topic = topic;
        this.transactions = new HashMap<>();
        this.randomAccessFile = new RandomAccessFile(topic.getTopicFile(), "rw");
        this.numberOfProducers = numberOfProducers;
    }

    public void put(String producerName, int value) {
        if (value <= 0) {
            this.handleTransactionOperation(producerName, value);
        } else {
            this.handleInsertOperation(producerName, value);
        }
    }

    private void handleInsertOperation(String producerName, int value) {
        if (this.transactions.containsKey(producerName)) {
            this.transactions.get(producerName).put(value);
        } else {
            synchronized (this) {
                this.writeValue(value + "");
                this.topic.notifyReaders();
                Logger.getLogger(Logger.GLOBAL_LOGGER_NAME).log(Level.INFO,
                        "producer:"+producerName+" send message:"+value);
            }

        }
    }

    private void handleTransactionOperation(String producerName, int value) {
        switch (value) {
            case 0 -> this.startTransaction(producerName);
            case -1 -> this.commitTransaction(producerName);
            case -2 -> this.cancelTransaction(producerName);
            case -3 -> this.endOneProducer(producerName);
        }
    }

    private void startTransaction(String producerName) {
        if (this.transactions.containsKey(producerName)) {
            Logger.getLogger(Logger.GLOBAL_LOGGER_NAME).log(Level.INFO,
                    "producer:"+producerName+" start transaction but dont finalize previous transaction.");
            this.commitTransaction(producerName);
        }else Logger.getLogger(Logger.GLOBAL_LOGGER_NAME).log(Level.INFO,
                "producer:"+producerName+" start transaction");
        this.transactions.put(producerName, new Transaction(this));
    }

    private void commitTransaction(String producerName) {
        if (this.transactions.containsKey(producerName)) {
            this.transactions.get(producerName).commit();
            this.transactions.remove(producerName);
            Logger.getLogger(Logger.GLOBAL_LOGGER_NAME).log(Level.INFO,
                    "producer:"+producerName+" commit transaction");
        } else {
            Logger.getLogger(Logger.GLOBAL_LOGGER_NAME).log(Level.INFO,
                    "producer:"+producerName+" commit a non-existing transaction");
        }
    }

    private void cancelTransaction(String producerName) {
        if (this.transactions.containsKey(producerName)) {
            this.transactions.remove(producerName);
            Logger.getLogger(Logger.GLOBAL_LOGGER_NAME).log(Level.INFO,
                    "producer:"+producerName+" cancel a transaction.");
        } else {
            Logger.getLogger(Logger.GLOBAL_LOGGER_NAME).log(Level.INFO,
                    "producer:"+producerName+" cancel a non-existing transaction.");
        }
    }

    private void endOneProducer(String producerName) {
        numberOfProducers += -1;
        Logger.getLogger(Logger.GLOBAL_LOGGER_NAME).log(Level.INFO,
                "producer "+producerName+" finished, topic name:"+topic.getName());
        if (numberOfProducers == 0) {
            topic.endTopic();
        }
    }

    void writeValue(String value) {
        try {
            this.randomAccessFile.writeUTF(value);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public Topic getTopic() {
        return topic;
    }
}
