package com.mercari.merpay.pubsub;

public class Publisher {
    private final String publisherId;
  
    public Publisher(String publisherId) {
      this.publisherId = publisherId;
    }
  
  public String getPublisherId() {
    return publisherId;
  }
}
