package Blob;


import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQSession;
import org.apache.activemq.BlobMessage;
import util.FileUtils;

import javax.jms.*;
import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * ��Ϣ������Ϣ������
 */
public class Producer {

    //ActiveMq ��Ĭ���û���
    private static final String USERNAME = ActiveMQConnection.DEFAULT_USER;
    //ActiveMq ��Ĭ�ϵ�¼����
    private static final String PASSWORD = ActiveMQConnection.DEFAULT_PASSWORD;
    //ActiveMQ �����ӵ�ַ
    private static final String BROKEN_URL = "tcp://localhost:61616?jms.blobTransferPolicy.defaultUploadUrl=http://localhost:8161/fileserver/";
    //���ӹ���
    ConnectionFactory connectionFactory;
    //���Ӷ���
    ActiveMQConnection  connection;
    //�������
    ActiveMQSession session;
    ThreadLocal<MessageProducer> threadLocal = new ThreadLocal<>();
    ExecutorService threadPool = Executors.newFixedThreadPool(8);
    /**
     * ��ʼ����Ϣ������
     * @throws JMSException
     */
    public  void init() throws JMSException {
        try {
            //����һ�����ӹ���
            ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(
                    USERNAME,PASSWORD,BROKEN_URL);
            //�ӹ����д���һ������
            connection  = (ActiveMQConnection)connectionFactory.createConnection();
            //��������
            connection.start();
            //����һ����������ͨ������������������ļ���
            session=(ActiveMQSession) connection.createSession(
                    false, Session.AUTO_ACKNOWLEDGE);
        } catch (JMSException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * ����Ϣ��������Ϣ����
     * @param disname
     * @param text
     */
    public void sendMessage(String disname,String text){
        try {
            //����һ����Ե���Ϣ����,������
            Destination queue = session.createQueue(disname);
            //��Ϣ������
            MessageProducer messageProducer = null;
            //������Ϣ������
            if(threadLocal.get()!=null){
                messageProducer = threadLocal.get();
            }else{
                messageProducer = session.createProducer(queue);
                messageProducer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);// ����Ϊ�ǳ־���
                threadLocal.set(messageProducer);
            }

            File file=new File("D:\\listener\\"+text);
            // ���� BlobMessage�����������ļ�
            BlobMessage msg = session.createBlobMessage(file);
            msg.setStringProperty("FILE.NAME", file.getName());
            msg.setLongProperty("FILE.SIZE", file.length());
            //����һ����ִ��ַ
            Destination reback=session.createQueue("reback");
            MessageConsumer reConsumer=session.createConsumer(reback);
            reConsumer.setMessageListener(new reListener(session));
            //����ִ��ַд����Ϣ
             msg.setJMSReplyTo(reback);
            //������Ϣ
            while (!file.renameTo(file)){
                //�����ļ����ڱ�����ʱ
                Thread.sleep(1000);
            }
            messageProducer.send(msg);
            //ɾ���ļ�
            if (file.exists()){
                file.delete();
            }
            System.out.println(text+"�ѷ�������Ϣ���У�");
//            messageProducer.close();
//            session.close();
//            connection.close(); // ���ر� Connection, �������˳�
        } catch (JMSException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
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
