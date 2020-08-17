package Consumer;

import Broker.MessageBroker;
import Broker.NoSuchTopicException;

import java.util.logging.Level;
import java.util.logging.Logger;

public class Consumer extends Thread {

    private final ConsumerGroup consumerGroup;
    private final String consumerName;

    Consumer(ConsumerGroup consumerGroup, String consumerName) {
        this.consumerGroup = consumerGroup;
        this.consumerName = consumerName;
        Logger.getLogger(Logger.GLOBAL_LOGGER_NAME).log(Level.INFO, "consumer created, consumer name:"+consumerName);
    }

    public int getMessage() throws NoSuchTopicException {
        return this.getMessageBroker().getMessage(getTopicName(), consumerGroup.getGroupName(), consumerName);
    }

    public void run() {
        while (true) {
            try {
                int message;
                if ((message=getMessage())==-10){
                    return;
                }
                consumerGroup.performAction(this, message);
            } catch (NoSuchTopicException e) {
                e.printStackTrace();
            }
        }
    }

    public MessageBroker getMessageBroker() {
        return consumerGroup.getMessageBroker();
    }

    public String getConsumerName() {
        return consumerName;
    }

    public String getTopicName() {
        return consumerGroup.getTopicName();
    }
}
