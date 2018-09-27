package org.kafka;

import java.io.InputStream;
import java.util.Properties;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
public class SimpleProducerWithProperties {
	private static final String PROP_FILENAME = "producer.properties";
	private static PropertiesConfiguration appProps;
   public static void main(String[] args) throws Exception{
           
      String topicName = "Test2";
	  String key = "Key1";
	  String value = "Value-1+ HI";
      
      Properties props = new Properties();
      InputStream input = null;
      Producer<String, String> producer = null;
      try {
    	  input = SimpleProducerWithProperties.class.getResourceAsStream("/resources/"+PROP_FILENAME);
          props.load(input);
	      producer = new KafkaProducer <>(props);
		  ProducerRecord<String, String> record = new ProducerRecord<>(topicName,key,value);
		  producer.send(record);	       
	      producer.close();
      }catch(Exception ex){
          ex.printStackTrace();
      }finally{
          input.close();
          producer.close();
      }
	  System.out.println("SimpleProducer Completed.");
   }
	public static PropertiesConfiguration getAppProps() {
		return appProps;
	}
	
	public static void setAppProps(PropertiesConfiguration appProps) {
		SimpleProducerWithProperties.appProps = appProps;
	}
}