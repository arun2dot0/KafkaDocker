package docker.kafka.test;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.IntStream;

//import kafka.javaapi.producer.SyncProducer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.javaapi.producer.Producer;
import kafka.message.Message;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.producer.SyncProducerConfig;
/**
 * Hello world!
 *
 */
public class SimpleProducer 
{
    public static void main( String[] args )
    {
        System.out.println( "Hello World!" );
        Properties props = new Properties();
     //   props.put("zk.connect", "192.168.99.100:2181");
      //  props.put("metadata.broker.list", "192.168.99.100:32775");
        props.put("zk.connect", "dfw-appblx097-01.prod.walmart.com:9181");
        props.put("metadata.broker.list", "dfw-appblx097-01.prod.walmart.com:9092,dfw-appblx098-05.prod.walmart.com:9092,	dfw-appblx098-06.prod.walmart.com:9092");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("partitioner.class", "docker.kafka.test.SimplePartitioner");
        props.put("request.required.acks", "1");

        
        ProducerConfig config = new ProducerConfig(props);
        Producer<String, String> producer = new Producer<String, String>(config);
        // The message is sent to a randomly selected partition registered in ZK
       // ProducerData<String, String> data = new ProducerData<String, String>("test", "test-message");
        
        IntStream.range(1, 1000).forEach(i->
        {
        	  KeyedMessage<String, String> data = new KeyedMessage<String, String>("Robotics", "test message "+i);
        	    producer.send(data);	
        });
//     
        System.out.println("All sent");
//        for( int i=0;i<100;i++)
//        {
//        	  KeyedMessage<String, String> data = new KeyedMessage<String, String>("test", "hello world");
//        	    producer.send(data);	
//        }
     
    }
}
