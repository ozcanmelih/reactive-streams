package com.mozcan.reactive_streams.main;

import com.mozcan.reactive_streams.publisher.TweetPublisher;
import com.mozcan.reactive_streams.subscriber.TweetSubscriber;

public class App {
	public static void main(String[] args) throws InterruptedException {
		TweetPublisher publisher = new TweetPublisher();
		TweetSubscriber subscriber = new TweetSubscriber("Client1", 1000, 5);

		publisher.subscribe(subscriber);
		
		Thread.sleep(30000);
		publisher.close();
	}
}
