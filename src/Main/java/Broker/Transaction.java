package Broker;

import java.util.LinkedList;
import java.util.Queue;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Transaction {

    private final TopicWriter topicWriter;
    private final Queue<Integer> values;

    Transaction(TopicWriter topicWriter) {
        this.topicWriter = topicWriter;
        values = new LinkedList<>();

    }

    void put(int value) {
        values.add(value);
    }

    void commit() {
        synchronized (this.topicWriter) {
            this.topicWriter.writeValue("start");
            while (!this.values.isEmpty()) {
                this.topicWriter.writeValue(values.remove()+"");
            }
            this.topicWriter.writeValue("end");
            this.topicWriter.getTopic().notifyReaders();
        }
    }
}
