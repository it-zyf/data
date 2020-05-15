package queue;

import org.junit.Test;

import java.util.LinkedList;
import java.util.Queue;

/**
 * @Author twh
 * @Date 2020/4/8 13:40
 */
public class QueueSequence {
    public static void main(String[] args) {
        // 这里暂时只开一个窗口。
        TaskQueue taskQueue = new TaskQueue(2);
        taskQueue.start();

    }
}
