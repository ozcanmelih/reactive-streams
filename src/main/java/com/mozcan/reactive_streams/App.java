package com.mozcan.reactive_streams;

/**
 * Hello world!
 *
 */
public class App {
	public static void main(String[] args) {
		TweetPublisher tweetPublisher = new TweetPublisher();
		TweetSubscriber tweetSubscriber = new TweetSubscriber();

		tweetPublisher.subscribe(tweetSubscriber);
	}
}
