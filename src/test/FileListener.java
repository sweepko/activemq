package test;


import ActiveMq.Consumer;
import ActiveMq.Producer;

import javax.jms.JMSException;
import java.io.IOException;
import java.nio.file.*;
import java.nio.file.WatchEvent.Kind;

import static java.nio.file.StandardWatchEventKinds.*;

public class FileListener {
    public static void main(String[] args) throws JMSException {
        //初始化主类对象
        ActiveMq.DirTest main=new ActiveMq.DirTest();
        //初始化生产者
        Producer producter = new Producer();
        producter.init();
        //初始化消费者对象
        Consumer comsumer = new Consumer();
        comsumer.init();
        //监听目录
        try (WatchService ws = FileSystems.getDefault().newWatchService()) {
            Path dirToWatch = Paths.get("D:\\listener");
            dirToWatch.register(ws, ENTRY_CREATE, ENTRY_MODIFY, ENTRY_DELETE);
            while (true) {
                WatchKey key = ws.take();
                for (WatchEvent<?> event : key.pollEvents()) {
                    Kind<?> eventKind = event.kind();
                    if (eventKind == OVERFLOW) {
                        System.out.println("Event  overflow occurred");
                        continue;
                    }
                    WatchEvent<Path> currEvent = (WatchEvent<Path>) event;
                    Path dirEntry = currEvent.context();
                    System.out.println(eventKind+" "+dirEntry);
                }
                boolean isKeyValid = key.reset();
                if (!isKeyValid) {
                    System.out.println("No  longer  watching " + dirToWatch);
                    break;
                }
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}
