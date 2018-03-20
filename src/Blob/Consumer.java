package Blob;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import javax.jms.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * ��Ϣ����������
 */
public class Consumer {
    private static final String USERNAME = ActiveMQConnection.DEFAULT_USER;

    private static final String PASSWORD = ActiveMQConnection.DEFAULT_PASSWORD;

    private static final String BROKEN_URL =  "tcp://localhost:61616";

    ConnectionFactory connectionFactory;

    Connection connection;

    Session session;

    ThreadLocal<MessageConsumer> threadLocal = new ThreadLocal<>();
    /**
     * ��ʼ����Ϣ�������������ӳء�����
     */
    public void init(){
        try {
            connectionFactory = new ActiveMQConnectionFactory(USERNAME,PASSWORD,"tcp://localhost:61616");
            connection  = connectionFactory.createConnection();
            connection.start();
            //��������
            session = connection.createSession(false,Session.CLIENT_ACKNOWLEDGE);
        } catch (JMSException e) {
            e.printStackTrace();
        }
    }

    /**
     * ������Ե���������Ϣ���У�ͨ��MessageListener������Ϣ����
     * ����ʽΪ"CLIENT_ACKNOWLEDGE",�ͻ���ȷ��֮��ŻὫ����Ϣ
     * ����Ϣ����ɾ��������acknowledge������
     * @param disname
     * @throws JMSException
     */
    public void getMessage(String disname) throws JMSException {
        //������Ե���Ϣ����
        Destination queue = session.createQueue(disname);
        MessageConsumer consumer = null;
        //������Ϣ�������ѷ�ʽ����������Ϣ��������
        if (threadLocal.get() != null) {
            consumer = threadLocal.get();
        } else {
            consumer = session.createConsumer(queue);
            consumer.setMessageListener(new Listener(session));
            threadLocal.set(consumer);
        }
   }
}
