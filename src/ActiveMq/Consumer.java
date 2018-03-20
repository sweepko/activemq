package ActiveMq;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;

/**
 * 消息队列消费者
 */
public class Consumer {
    private static final String USERNAME = ActiveMQConnection.DEFAULT_USER;

    private static final String PASSWORD = ActiveMQConnection.DEFAULT_PASSWORD;

    private static final String BROKEN_URL =  "tcp://10.66.10.36:61616";

    ConnectionFactory connectionFactory;

    Connection connection;

    Session session;

    ThreadLocal<MessageConsumer> threadLocal = new ThreadLocal<>();
    /**
     * 初始化消息队列消费者连接池、事务
     */
    public void init(){
        try {
            connectionFactory = new ActiveMQConnectionFactory(USERNAME,PASSWORD,"tcp://10.66.10.36:61616");
            connection  = connectionFactory.createConnection();
            connection.start();
            //创建事务
            session = connection.createSession(false,Session.AUTO_ACKNOWLEDGE);
        } catch (JMSException e) {
            e.printStackTrace();
        }
    }

    /**
     * 建立点对点消费者消息队列，通过MessageListener监听消息队列
     * 事务方式为"CLIENT_ACKNOWLEDGE",客户端确认之后才会将该消息
     * 从消息队列删除（调用acknowledge方法）
     * @param disname
     * @throws JMSException
     */
    public void getMessage(String disname) throws JMSException {
        //创建点对点消息队列
        Destination queue = session.createQueue(disname);
        MessageConsumer consumer = null;
        //设置消息队列消费方式（这里是消息监听器）
        if (threadLocal.get() != null) {
            consumer = threadLocal.get();
        } else {
            consumer = session.createConsumer(queue);
            consumer.setMessageListener(new Listener(session));
            threadLocal.set(consumer);
        }
   }
}
