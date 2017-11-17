package SearchAdsFeeder;
import com.rabbitmq.client.ConnectionFactory;


import com.rabbitmq.client.Connection;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;


import com.rabbitmq.client.Channel;

public class SearchAdsFeederMain {

    private static void publishCrawlerFeed() throws Exception {

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("127.0.0.1");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        channel.queueDeclare("q_feeds", true, false, false, null);
        //class Feed {String category; String url;}
        
        String feedersMsg = "lego city toy,6.5, 8120,17";
        System.out.println(" [x] Sent '" + feedersMsg + "'");
	    channel.basicPublish("", "q_feeds", null, feedersMsg.getBytes("UTF-8"));
//        
//        String feederFilePath ="/home/vagrant/eclipse-workspace/week6/SearchAdsFeeder/rawQuery3.txt";
//        try (BufferedReader brFeeders = new BufferedReader(new FileReader(feederFilePath))) {
//			String line;
//			while ((line = brFeeders.readLine()) != null) {
//				String feedersMsg=line;
//				 System.out.println(" [x] Sent '" + feedersMsg + "'");
//			     channel.basicPublish("", "q_feeds", null, feedersMsg.getBytes("UTF-8"));
//			}
//		}catch (IOException e) {
//			e.printStackTrace();
//			channel.close();
//	        connection.close();
//		}

        channel.close();
        connection.close();
    }


    public static void main(String[] args) throws Exception{
     
        publishCrawlerFeed();
    }
}
