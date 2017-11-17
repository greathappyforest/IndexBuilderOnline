package searchAdsCrawler;



import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeoutException;

import searchAdsCrawler.Ad;
import searchAdsCrawler.Utility;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.rabbitmq.client.*;


public class searchAdsCrawlerMain {

		private final static String IN_QUEUE_NAME = "q_feeds";
	    private final static String OUT_QUEUE_NAME = "q_product";
	    private final static String ERR_QUEUE_NAME = "q_error";

	    private static AmazonCrawler crawler;
	    private static ObjectMapper mapper;
	    private static Channel outChannel;
	    private static Channel errChannel;
	  
	    
	    public static void main(String[] args) throws IOException,TimeoutException,InterruptedException {

	        mapper = new ObjectMapper();

	        String proxyFilePath = "../proxylist_bittiger.csv";

	        ConnectionFactory factory = new ConnectionFactory();
	        factory.setHost("localhost");
	        Connection connection = factory.newConnection();
	        Channel inChannel = connection.createChannel();
	        inChannel.queueDeclare(IN_QUEUE_NAME, true, false, false, null);
	        inChannel.basicQos(10); // Per consumer limit
	        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");
	        outChannel = connection.createChannel();
	        outChannel.queueDeclare(OUT_QUEUE_NAME, true, false, false, null);

	        errChannel = connection.createChannel();
	        errChannel.queueDeclare(ERR_QUEUE_NAME, true, false, false, null);


	        
	        //callback
	        Consumer consumer = new DefaultConsumer(inChannel) {
	            @Override
	            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
	                    throws IOException {
	                try {
	                    String message = new String(body, "UTF-8");
	                    System.out.println(" [x] Received '" + message + "'");
	                    String[] fields = message.split(",");
	                    String query = fields[0].trim();
	                    double bidPrice = Double.parseDouble(fields[1].trim());
	                    int campaignId = Integer.parseInt(fields[2].trim());
	                    int queryGroupId = Integer.parseInt(fields[3].trim()); 
	                    crawler = new AmazonCrawler(proxyFilePath, errChannel, ERR_QUEUE_NAME);
	                    List<Ad> ads =  crawler.GetAdBasicInfoByQuery(query, bidPrice, campaignId, queryGroupId);
	                //    System.out.println(ads.size());
	                    for(Ad ad : ads) {
	                    	ad.keyWords = Utility.cleanedTokenize(ad.title);
	                        String jsonInString = mapper.writeValueAsString(ad);
	                        //System.out.println(jsonInString);
	                        
	                  
	                        
	                        outChannel.basicPublish("", OUT_QUEUE_NAME, null, jsonInString.getBytes("UTF-8"));
	                    }
	                    Thread.sleep(1000);
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
	        inChannel.basicConsume(IN_QUEUE_NAME, true, consumer);

	        //bw.close();

	        /*
	        try (BufferedReader br = new BufferedReader(new FileReader(rawQueryDataFilePath))) {

	            String line;
	            while ((line = br.readLine()) != null) {
	                if(line.isEmpty())
	                    continue;
	                System.out.println(line);
	                String[] fields = line.split(",");
	                String query = fields[0].trim();
	                double bidPrice = Double.parseDouble(fields[1].trim());
	                int campaignId = Integer.parseInt(fields[2].trim());
	                int queryGroupId = Integer.parseInt(fields[3].trim());

	                List<Ad> ads =  crawler.GetAdBasicInfoByQuery(query, bidPrice, campaignId, queryGroupId);
	                for(Ad ad : ads) {
	                    String jsonInString = mapper.writeValueAsString(ad);
	                    //System.out.println(jsonInString);
	                    bw.write(jsonInString);
	                    bw.newLine();
	                }
	                Thread.sleep(2000);
	            }
	            bw.close();
	        }catch(InterruptedException ex) {
	            Thread.currentThread().interrupt();
	        } catch (JsonGenerationException e) {
	            e.printStackTrace();
	        } catch (JsonMappingException e) {
	            e.printStackTrace();
	        } catch (IOException e) {
	            e.printStackTrace();
	        }*/

	        //crawler.cleanup();
	        //inChannel.close();
	        //outChannel.close();
	        //connection.close();
	        //connection2.close();

	    }
	    
	    
	    
	    
	}
