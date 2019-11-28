package spark.streaming.structured.i_TwitterStreaming

/**************
 * Author : Prudhvi Akella.
 * Desc:Twitter Streams output to Socket
 */

import java.io.ObjectOutputStream
import java.net.{ServerSocket, Socket}

import twitter4j.{FilterQuery, StallWarning, Status, StatusDeletionNotice, StatusListener, TwitterStream, TwitterStreamFactory}
import twitter4j.conf.ConfigurationBuilder

object a_TwitterStreaming extends App{
  //Creating ServerSocket Connection
  val server = new ServerSocket(9999)
  println("Waiting for the client request")
  //Accept the connections from client
  //It waits here until connection.Once the connection is established then only remaining code
  //will execute.
  val socket = server.accept
  System.out.println("Connected");
  //Creating ObjectOutputStream to write the twitter messages to socket
  val oos = new ObjectOutputStream(socket.getOutputStream)
  //Creating ConfigurationBuilder which hold the consumerKey,Secret,AccessToken,Key of twitter account
  //which will be used for authentication and authorization
  val cb = new ConfigurationBuilder();
  cb.setDebugEnabled(true).setOAuthConsumerKey("FFBW9PQKUM8VtEy0s28uWEOE1")
    .setOAuthConsumerSecret("IKxDk2X0fUGKjCsLMRGtezQ95UJ3JO7UEPnMEeiIBcXst1pVof")
    .setOAuthAccessToken("988237601625686016-XFfcZFRrKb8foSUSN5IWmzlff7EGXQ5")
    .setOAuthAccessTokenSecret("daXdevb9ydZLHeYVuzCmzg87Tw5m5j85l7McfLzkKUV6y");
  //Create a Twitter Stream with configuration builder
  val twitterStream = new TwitterStreamFactory(cb.build())
    .getInstance();
  //Creating a listener for stream
  val listener = new StatusListener() {
    //When ever user posts the new message or when ever tacker matches with any of new messages in twitter
    //that will be tracked through status.
    def onStatus(status:Status) {
      println("@" + status.getUser().getScreenName() + " - " + status.getText());
      //write object to Socket
      oos.writeObject(status.getText())
    }
    def onDeletionNotice( statusDeletionNotice:StatusDeletionNotice) {
      println("Got a status deletion notice id:" + statusDeletionNotice.getStatusId());
    }

    def onTrackLimitationNotice( numberOfLimitedStatuses:Int) {
      println("Got track limitation notice:" + numberOfLimitedStatuses);
    }
    def onScrubGeo( userId:Long,  upToStatusId:Long) {
      println("Got scrub_geo event userId:" + userId + " upToStatusId:" + upToStatusId);
    }

    def onStallWarning( warning:StallWarning) {
      println("Got stall warning:" + warning);
    }
    def onException( ex:Exception) {
      ex.printStackTrace();
    }
  };
  twitterStream.addListener(listener);
  twitterStream.sample();
  //Use this if you want follow tweets of a user
  //twitterStream.filter(new FilterQuery().follow(1344951).follow(5988062))
  //Use this if you want to track a keywork.
  twitterStream.filter(new FilterQuery().track("fifa","nba","ipl"))
}
