package com.mercari.merpay.pubsub;

public class Message {
  
  private final String messageId;
  private final String message;
  
  public Message(String messageId, String message) {
    this.messageId = messageId;
    this.message = message;
  }
  
  public String getMessage(){
    return this.message;
  }
  
  public String getMessageId() {
    return messageId;
  }
}
