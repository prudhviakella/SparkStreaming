package spark.streaming.structured.i_TwitterStreaming

import java.io.DataOutputStream
import java.net.{ServerSocket, Socket}

import twitter4j.{FilterQuery, StallWarning, Status, StatusDeletionNotice, StatusListener, TwitterStream, TwitterStreamFactory}
import twitter4j.conf.ConfigurationBuilder

object TwitterStreaming extends App{

  val server = new ServerSocket(9999)
  println("Waiting for the client request")
  val socket = server.accept

  System.out.println("Connected");
  val cb = new ConfigurationBuilder();
  cb.setDebugEnabled(true).setOAuthConsumerKey("FFBW9PQKUM8VtEy0s28uWEOE1")
    .setOAuthConsumerSecret("IKxDk2X0fUGKjCsLMRGtezQ95UJ3JO7UEPnMEeiIBcXst1pVof")
    .setOAuthAccessToken("988237601625686016-XFfcZFRrKb8foSUSN5IWmzlff7EGXQ5")
    .setOAuthAccessTokenSecret("daXdevb9ydZLHeYVuzCmzg87Tw5m5j85l7McfLzkKUV6y");
  val twitterStream = new TwitterStreamFactory(cb.build())
    .getInstance();
  val listener = new StatusListener() {
    def onStatus(status:Status) {
      println("@" + status.getUser().getScreenName() + " - " + status.getText());
      import java.io.ObjectOutputStream
      val oos = new ObjectOutputStream(socket.getOutputStream)
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
  twitterStream.filter(new FilterQuery().follow(1344951).follow(5988062))
  }
