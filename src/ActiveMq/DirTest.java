package ActiveMq;

import javax.jms.JMSException;
import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.nio.file.WatchEvent.Kind;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static java.nio.file.StandardWatchEventKinds.*;

public class DirTest {
    public static void main(String[] args) throws JMSException {
        ExecutorService cachedThreadPool = Executors.newCachedThreadPool();
        //初始化主类对象
        DirTest main=new DirTest();
        //初始化生产者
        Producer producter = new Producer();
        producter.init();
        //初始化消费者对象
        Consumer comsumer = new Consumer();
        comsumer.init();
        //从缓存线程池取出线程，启动消费线程，进行移动文件
        cachedThreadPool.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    comsumer.getMessage("sweep");
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }
        });
//        new Thread(main.new ConsumerMq(comsumer)).start();
        //如果目录下有文件先移出来
        String path="D:\\listener";
        File file=new File(path);
        for (File temp:file.listFiles()){
            if (temp.isFile()){
                //创建生产者线程
                cachedThreadPool.execute(new Runnable() {
                    @Override
                    public void run() {
                        producter.sendMessage("sweep",temp.getName());
                    }
                });
//                new Thread(main.new ProductorMq(producter,temp.getName())).start();
            }
        }
        //监听目录
        try (WatchService ws = FileSystems.getDefault().newWatchService()) {
            Path dirToWatch = Paths.get(path);
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
                    if ("CREATE".equals(eventKind.toString().substring(6,12))){
//                        System.out.println("目录新成员:"+dirEntry.toString());
                        //创建线程将新件名发送至消息队列
                        cachedThreadPool.execute(new Runnable() {
                            @Override
                            public void run() {
                                producter.sendMessage("sweep",dirEntry.toString());
                            }
                        });
                    }
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
    //生产者线程
//    private class ProductorMq implements Runnable {
//        Producer producter;
//        String str;
//        //param1:消息队列生产者对象；param2:目录里面的文件名
//        public ProductorMq(Producer producter, String str) {
//            this.producter = producter;
//            this.str=str;
//        }
//        @Override
//        public void run() {
//            producter.sendMessage("sweep",str);
//        }
//    }
    //消费者线程
//    private class ConsumerMq implements Runnable {
//        Consumer comsumer;
//
//        public ConsumerMq(Consumer comsumer) {
//            this.comsumer = comsumer;
//        }
//        @Override
//        public void run() {
//            try {
//                while(true){
//                    try {
//                        Thread.sleep(1000);
//                    } catch (InterruptedException e) {
//                        e.printStackTrace();
//                    }
//                    comsumer.getMessage("sweep");
//                }
//            } catch (JMSException e) {
//                e.printStackTrace();
//            }
//        }
//    }
//}