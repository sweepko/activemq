package ActiveMq;

import org.apache.activemq.BlobMessage;
import util.FileUtils;

import javax.jms.*;
import java.io.*;
import java.sql.Blob;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


/**
 * 消息队列监听器
 */
public class Listener implements MessageListener {
    private Session session;
    public Listener(Session session){
        this.session=session;
    }
    //创建一个可重用固定线程数的线程池，以共享的无界队列方式来运行这些线程。
    private ExecutorService threadPool = Executors.newFixedThreadPool(8);
    private long startTime=System.currentTimeMillis();
    @Override
    public void onMessage(Message message) {
        threadPool.execute(new Runnable() {
            @Override
            public void run() {
                String fileName="";
                FileOutputStream out=null;
                try {
                    if (message instanceof BytesMessage) {
                        BytesMessage bytesMessage= (BytesMessage) message;
                        fileName=bytesMessage.getStringProperty("FileName");
                        out=new FileOutputStream("D:\\sink\\"+fileName);
                        byte[] bytes=new byte[1024];
                        int len=0;
                        while ((len=bytesMessage.readBytes(bytes))!=-1){
                            out.write(bytes,0,len);
                        }
                        //获得回执地址
                        Destination recall_destination = message.getJMSReplyTo();
                        // 创建回执消息
                        TextMessage textMessage = session.createTextMessage(fileName+"已处理完毕");
                        // 以上收到消息之后，从新创建生产者，然后在回执过去
                        MessageProducer producer = session.createProducer(recall_destination);
                        producer.send(textMessage);
                    }
                } catch (JMSException e) {
                    //递归
                    if (System.currentTimeMillis()-startTime<=15000){
                        onMessage(message);
                    }
                    else{
                        System.out.println(fileName+"文件传输失败，有可能是文件过大！");
                    }
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    System.out.println(fileName+"传输失败！");
                }finally {
                    try {
                        out.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        });
    }
}
