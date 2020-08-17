public class Guardian {
    private final Object lock;
    private boolean free;

    public Guardian(){
        lock = new Object();
        free = true;
    }


    public void acquire() {
        synchronized (lock) {
            while (!free) {
                try {
                    lock.wait();
                } catch (InterruptedException ignore) {
                    //e.printStackTrace();
                    // can be ignored
                }
            }
            free = false;
        }
    }

    public void release() {
        synchronized (lock) {
            lock.notifyAll();
            free = true;
        }
    }

//    public void writeFile(Node mainNode, PrintWriter w) {
//        if (mainNode == null)  // base case to stop recursion
//            return;
//        w.print("{");
//        writeFile(mainNode.leftChild, w);
//        w.print("}");
//        w.print(mainNode);
//        w.print("{");
//        writeFile(mainNode.rightChild, w);
//        w.print("}");
//    }
}
