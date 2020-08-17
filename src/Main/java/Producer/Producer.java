package Producer;

import Broker.MessageBroker;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

public class Producer extends Thread{
    private final MessageBroker messageBroker;
    private final String topicName;
    private final String producerName;
    private final File producerFile;

    Producer(MessageBroker messageBroker, String topicName, String producerName, File producerFile) {
        this.messageBroker = messageBroker;
        this.topicName = topicName;
        this.producerName = producerName;
        this.producerFile = producerFile;
    }

    public void run() {
        try {
            Scanner scanner = new Scanner(producerFile);
            while(scanner.hasNext()) {
                put(scanner.nextInt());
            }
            put(-3);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    private void put(int value) {
        messageBroker.put(topicName, producerName, value);
    }
}
