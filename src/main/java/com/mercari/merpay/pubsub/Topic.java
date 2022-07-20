package com.mercari.merpay.pubsub;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

// Monitor for addingMessage on owner
// Monitor for TopicSubscriber when reading/ack
public class Topic {
      
      private final Publisher owner;
      private final ConcurrentHashMap<String, TopicSubscriber> subscriberMap;
      private final List<Message> messageList;
      private final ReentrantReadWriteLock messageListLock;
      
      public Topic(String publisherId) {
            this.owner = new Publisher(publisherId);
            this.subscriberMap = new ConcurrentHashMap<>();
            this.messageList = new ArrayList<>();
            messageListLock = new ReentrantReadWriteLock(true);
          
      }
      
      public void addMessage(String message, String messageId){
            synchronized (owner) {
                  messageListLock.writeLock().lock();
                  try {
                        this.messageList.add(new Message(messageId, message));
                  }finally {
                        messageListLock.writeLock().unlock();
                  }
            }
      }
      
      public boolean authenticate(String publisherId){
            return (publisherId == owner.getPublisherId());
      }
      
      public void addSubscriber(String subscriberId){
            assert(!subscriberMap.containsKey(subscriberId));
            TopicSubscriber topicSubscriber = new TopicSubscriber(subscriberId);
            subscriberMap.put(subscriberId, topicSubscriber);
      }
      
      private int getMessageListSize(){
            messageListLock.readLock();
            int sz =  messageList.size();
            messageListLock.readLock().unlock();
            return sz;
      }
      
      public void ackMessage(String subscriberId, String messageId) throws Exception{
            assert(subscriberMap.containsKey(subscriberId));
            TopicSubscriber topicSubscriber = subscriberMap.get(subscriberId);
            synchronized (topicSubscriber){
                  int sz = getMessageListSize();
                  if(topicSubscriber.getOffset() >= sz){
                        throw  new RuntimeException("Cannot ack");
                  }
                  messageListLock.readLock().lock();
                  Message message = messageList.get(topicSubscriber.getOffset());
                  if(message.getMessageId() != messageId){
                        throw new RuntimeException("Cannot ack");
                  }
                  topicSubscriber.incrementOffset();
                  messageListLock.readLock().unlock();
            }
      }
      
      public String getMessage(String subscriberId){
            assert(subscriberMap.containsKey(subscriberId));
            TopicSubscriber topicSubscriber = subscriberMap.get(subscriberId);
            synchronized (topicSubscriber){
                  int sz = getMessageListSize();
                  if(topicSubscriber.getOffset() >= sz){
                        throw new RuntimeException("No New Message");
                  }
                  messageListLock.readLock().lock();
                  Message message = messageList.get(topicSubscriber.getOffset());
                  messageListLock.readLock().unlock();
                  return message.getMessage();
            }
            
      }
      
      public static class TopicSubscriber{
            private final String SubscriberId;
            private Integer offset;
            
            public TopicSubscriber(String subscriberId) {
                  SubscriberId = subscriberId;
                  offset = 0;
            }
      
            public int getOffset() {
                  return offset;
            }
      
            public String getSubscriberId() {
                  return SubscriberId;
            }
            
            public void incrementOffset(){
                  offset++;
            }
      }
      
}
