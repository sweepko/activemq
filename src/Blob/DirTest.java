package Blob;

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
        //��ʼ���������
        DirTest main=new DirTest();
        //��ʼ��������
        Producer producter = new Producer();
        producter.init();
        //��ʼ�������߶���
        Consumer comsumer = new Consumer();
        comsumer.init();
        //�ӻ����̳߳�ȡ���̣߳����������̣߳������ƶ��ļ�
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
        //���Ŀ¼�����ļ����Ƴ���
        String path="D:\\listener";
        File file=new File(path);
        for (File temp:file.listFiles()){
            if (temp.isFile()){
                //�����������߳�
                cachedThreadPool.execute(new Runnable() {
                    @Override
                    public void run() {
                        producter.sendMessage("sweep",temp.getName());
                    }
                });
//                new Thread(main.new ProductorMq(producter,temp.getName())).start();
            }
        }
        //����Ŀ¼
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
//                        System.out.println("Ŀ¼�³�Ա:"+dirEntry.toString());
                        //�����߳̽��¼�����������Ϣ����
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
    //�������߳�
//    private class ProductorMq implements Runnable {
//        Producer producter;
//        String str;
//        //param1:��Ϣ���������߶���param2:Ŀ¼������ļ���
//        public ProductorMq(Producer producter, String str) {
//            this.producter = producter;
//            this.str=str;
//        }
//        @Override
//        public void run() {
//            producter.sendMessage("sweep",str);
//        }
//    }
    //�������߳�
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