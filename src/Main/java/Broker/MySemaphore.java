package Broker;

public class MySemaphore {
    private final Object object;
    private int signals;
    private boolean free;

    public MySemaphore() {
        object = new Object();
        free = true;
        signals = 0;
    }

    public void addSignal(int a) {
        synchronized (object) {
            signals += a;
            object.notifyAll();
        }
    }

    public void acquire() {
        synchronized (object) {
            while (signals <= 0 || !free) {
                try {
                    object.wait();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            signals--;
            free = false;
        }
    }

    public void release() {
        synchronized (object) {
            free = true;
            object.notifyAll();
        }
    }
}
