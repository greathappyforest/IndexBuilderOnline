package indexbuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;

import org.json.JSONArray;
import org.json.JSONObject;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

import indexbuilder.Ad;
import indexbuilder.Utility;
import indexbuilder.IndexBuilder;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;


public class IndexBuilderConsumerMain {
	private final static String IN_QUEUE2_NAME = "q_product";
  //  private final static String OUT_QUEUE2_NAME = "q_index_consumer";
    private final static String ERR_QUEUE2_NAME = "q_error2";

    private static ObjectMapper mapper;
    private static Channel outChannel;
    private static Channel errChannel;
    
    private static String memcachedServer ="127.0.0.1";
    private static int memcachedPortal =11211;
    private static String mysql_host = "127.0.0.1:3306";
    private static String mysql_db = "searchads";
    private static String mysql_user = "root";
    private static String mysql_pass = "password";
    private static MySQLAccess mysql =new MySQLAccess(mysql_host, mysql_db, mysql_user, mysql_pass);
    
    private static IndexBuilder indexBuilder = new IndexBuilder(memcachedServer,memcachedPortal,mysql_host,mysql_db,mysql_user,mysql_pass);
	
    
	public static void main(String[] args) throws IOException, TimeoutException {
		   
		   mapper = new ObjectMapper();


	        ConnectionFactory factory = new ConnectionFactory();
	        factory.setHost("localhost");
	        Connection connection = factory.newConnection();
	        Channel inChannel = connection.createChannel();
	        inChannel.queueDeclare(IN_QUEUE2_NAME, true, false, false, null);
	        inChannel.basicQos(10); // Per consumer limit
	        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");
	  

	        errChannel = connection.createChannel();
	        errChannel.queueDeclare(ERR_QUEUE2_NAME, true, false, false, null);

	        //callback
	        Consumer consumer = new DefaultConsumer(inChannel) {
	            @Override
	            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
	                    throws IOException {
	                try {
	                    String message = new String(body, "UTF-8");
	                    System.out.println(" [x] Received '" + message + "'");
	                  
	                	JSONObject adJson = new JSONObject(message);
	                
	    				if(!adJson.isNull("adId") && !adJson.isNull("campaignId")) {
	    					Ad ad = new Ad(); 
		    				ad.adId = adJson.getLong("adId");
		    				ad.campaignId = adJson.getLong("campaignId");
		    				ad.brand = adJson.isNull("brand") ? "" : adJson.getString("brand");
		    				ad.price = adJson.isNull("price") ? 100.0 : adJson.getDouble("price");
		    				ad.thumbnail = adJson.isNull("thumbnail") ? "" : adJson.getString("thumbnail");
		    				ad.title = adJson.isNull("title") ? "" : adJson.getString("title");
		    				ad.detail_url = adJson.isNull("detail_url") ? "" : adJson.getString("detail_url");						
		    				ad.bidPrice = adJson.isNull("bidPrice") ? 1.0 : adJson.getDouble("bidPrice");
		    				ad.pClick = adJson.isNull("pClick") ? 0.0 : adJson.getDouble("pClick");
		    				ad.category =  adJson.isNull("category") ? "" : adJson.getString("category");
		    				ad.description = adJson.isNull("description") ? "" : adJson.getString("description");
		    				ad.keyWords = new ArrayList<String>();
		    				JSONArray keyWords = adJson.isNull("keyWords") ? null :  adJson.getJSONArray("keyWords");
		    				for(int j = 0; j < keyWords.length();j++)
		    				{
		    					ad.keyWords.add(keyWords.getString(j));
		    				}

		    				String jsonInString = mapper.writeValueAsString(ad);
		    			
	    					indexBuilder.buildInvertIndex(ad);
	    			
	    					if(!indexBuilder.buildInvertIndex(ad) || !indexBuilder.buildForwardIndex(ad)){}
		    					
	    				}
	    				
	    				
	                    Thread.sleep(200);
	                }catch(InterruptedException ex) {
	                    Thread.currentThread().interrupt();
	                }  catch (IOException e) {
	                    e.printStackTrace();
	                } catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
	            }
	        };
	        inChannel.basicConsume(IN_QUEUE2_NAME, true, consumer);


	}

}
