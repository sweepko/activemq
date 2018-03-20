package Blob;

import org.apache.activemq.BlobMessage;
import util.FileUtils;

import javax.jms.*;
import java.io.*;
import java.sql.Blob;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


/**
 * ��Ϣ���м�����
 */
public class Listener implements MessageListener {
    private Session session;
    public Listener(Session session){
        this.session=session;
    }
    //����һ�������ù̶��߳������̳߳أ��Թ�����޽���з�ʽ��������Щ�̡߳�
    private ExecutorService threadPool = Executors.newFixedThreadPool(8);
    private long startTime=System.currentTimeMillis();
    @Override
    public void onMessage(Message message) {
        threadPool.execute(new Runnable() {
            @Override
            public void run() {
                String fileName="";
                try {
                    if (message instanceof BlobMessage){
                        BlobMessage blobMessage=(BlobMessage)message;
                        fileName = blobMessage.getStringProperty("FILE.NAME");
                        //��û�ִ��ַ
                        Destination recall_destination = message.getJMSReplyTo();
                        //д�ļ�
                        InputStream inputStream = blobMessage.getInputStream();
                        OutputStream os = new FileOutputStream("D:\\sink\\"+fileName);
                        // д�ļ�����Ҳ����ʹ��������ʽ
                        byte[] buff = new byte[256];
                        int len = 0;
                        while ((len = inputStream.read(buff)) > 0) {
                            os.write(buff, 0, len);
                        }
                        os.close();
                        message.acknowledge();
                        // ������ִ��Ϣ
                        TextMessage textMessage = session.createTextMessage(fileName+"�Ѵ������");
                        // �����յ���Ϣ֮�󣬴��´��������ߣ�Ȼ���ڻ�ִ��ȥ
                        MessageProducer producer = session.createProducer(recall_destination);
                        producer.send(textMessage);
                            //ÿ�����룬���´����߳�������Ϣ���е���Ϣ
//                            try {
//                                Thread.sleep(2000);
//                            } catch (InterruptedException e) {
//                                e.printStackTrace();
//                            }
//                            long time=System.currentTimeMillis()-startTime;
//                            System.out.println(time);
//                            if (time<=15000){
//                                onMessage(message);
//                            }else{
//                                System.out.println("�Բ����ļ�����ʧ�ܣ��п������ļ�����");
//                    }
                    }
                } catch (JMSException e) {
                    //�ݹ�
                    if (System.currentTimeMillis()-startTime<=15000){
                        onMessage(message);
                    }
                    else{
                        System.out.println(fileName+"�ļ�����ʧ�ܣ��п������ļ�����");
                    }
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    System.out.println(fileName+"����ʧ�ܣ�");
                }
            }
        });
    }
}
