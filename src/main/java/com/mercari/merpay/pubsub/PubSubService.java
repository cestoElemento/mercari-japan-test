package com.mercari.merpay.pubsub;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

// API for PubSubService
// Concurrent access map for fast read
public class PubSubService {
  
  final ConcurrentHashMap<String, Topic> topicMap;
  
  public PubSubService(){
    topicMap = new ConcurrentHashMap<>();
  }
  
  public Topic getTopicFromTopicMap(String topic){
    return topicMap.get(topic);
  }

  public boolean registerTopic(String publisherId, String topicId) throws Exception{
      assert(!topicMap.containsKey(topicId));
      Topic topic = new Topic(publisherId);
      topicMap.put(topicId, topic);
      return true;
  }
  
  public  boolean publish(String publisherId,  String  message, String messageId, String topicId) throws  Exception{
      assert(topicMap.containsKey(topicId));
      Topic topic = topicMap.get(topicId);
      if (!topic.authenticate(publisherId)) {
        throw new RuntimeException("Not Authenticated");
      }
      topic.addMessage(message, messageId);
      return true;
  }
  
  public  boolean subscribe(String subScriberId, String topicId) throws  Exception{
      assert(topicMap.containsKey(topicId));
      Topic topic = topicMap.get(topicId);
      topic.addSubscriber(subScriberId);
      return true;
  }
  
  public String getMessage(String subscriberId, String topicId){
      Topic topic = getTopicFromTopicMap(topicId);
      return topic.getMessage(subscriberId);
  }
  
  public void ack(String subscriberId, String topicId, String messageId) throws Exception{
      Topic topic = getTopicFromTopicMap(topicId);
      topic.ackMessage(subscriberId, messageId);
  }
  

}
