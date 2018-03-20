package ActiveMq;


import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQSession;
import org.apache.activemq.BlobMessage;
import util.FileUtils;

import javax.jms.*;
import java.io.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 消息队列消息生产者
 */
public class Producer {

    //ActiveMq 的默认用户名
    private static final String USERNAME = ActiveMQConnection.DEFAULT_USER;
    //ActiveMq 的默认登录密码
    private static final String PASSWORD = ActiveMQConnection.DEFAULT_PASSWORD;
    //ActiveMQ 的链接地址
    private static final String BROKEN_URL = "tcp://10.66.10.36:61616?jms.blobTransferPolicy.defaultUploadUrl=http://10.66.10.36:8161/fileserver/";
    //链接工厂
    ConnectionFactory connectionFactory;
    //链接对象
    ActiveMQConnection  connection;
    //事务管理
    ActiveMQSession session;
    ThreadLocal<MessageProducer> threadLocal = new ThreadLocal<>();
    ExecutorService threadPool = Executors.newFixedThreadPool(8);
    /**
     * 初始化消息生产者
     * @throws JMSException
     */
    public  void init() throws JMSException {
        try {
            //创建一个链接工厂
            ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(
                    USERNAME,PASSWORD,"tcp://10.66.10.36:61616");
            //从工厂中创建一个链接
            connection  = (ActiveMQConnection)connectionFactory.createConnection();
            //开启链接
            connection.start();
            //创建一个事务（这里通过参数可以设置事务的级别）
            session=(ActiveMQSession) connection.createSession(
                    false, Session.AUTO_ACKNOWLEDGE);
        } catch (JMSException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 将消息发送至消息队列
     * @param disname
     * @param text
     */
    public void sendMessage(String disname,String text) {
        InputStream is=null;
        try {
            //创建一个点对点消息队列,队列名
            Destination queue = session.createQueue(disname);
            //消息生产者
            MessageProducer messageProducer = null;
            //设置消息生产者
            if(threadLocal.get()!=null){
                messageProducer = threadLocal.get();
            }else{
                messageProducer = session.createProducer(queue);
                threadLocal.set(messageProducer);
            }

            File file=new File("D:\\listener\\"+text);
            while (!file.renameTo(file)){
                //当该文件正在被操作时
                Thread.sleep(1000);
            }
            MessageProducer producer = session.createProducer(queue);
            BytesMessage bytesMessage=session.createBytesMessage();
            is = new FileInputStream(file);
            // 读取数据到byte数组中
            byte[] buffer=new byte[is.available()];
            is.read(buffer);
            bytesMessage.writeBytes(buffer);

            //创建一个回执地址
            Destination reback=session.createQueue("reback");
            MessageConsumer reConsumer=session.createConsumer(reback);
            reConsumer.setMessageListener(new reListener(session));
            //将回执地址写到消息
            bytesMessage.setJMSReplyTo(reback);
            bytesMessage.setStringProperty("FileName",text);
            producer.send(bytesMessage);
            is.close();
            //删除文件
            if (file.exists()&&file.isFile()){
                file.delete();
            }
            System.out.println(file.getName()+"发送消息成功！");

        } catch (JMSException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }finally {

        }
    }

    private class reListener implements MessageListener {
        ActiveMQSession session=null;
        public reListener(ActiveMQSession session) {
            this.session=session;
        }

        @Override
        public void onMessage(Message message) {
            threadPool.execute(new Runnable() {
                @Override
                public void run() {
                    TextMessage tx= (TextMessage) message;
                    try {
                        System.out.println(tx.getText()+"----");
                    } catch (JMSException e) {
                        e.printStackTrace();
                    }
                }
            });
        }
    }
}
