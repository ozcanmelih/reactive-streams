package com.mozcan.reactive_streams;

import java.util.UUID;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.mozcan.reactive_streams.model.Tweet;

public class TweetSubscriber implements Subscriber<Tweet> {

	private Logger logger = Logger.getLogger(getClass().getName());

	private final String id = UUID.randomUUID().toString();
	private Subscription subscription;
	private static final int SUB_REQ = 5; // backpressure
	private static final int SLEEP = 1000;

	public void onSubscribe(Subscription subscription) {
		logger.info("subscriber: " + id + " ==> Subscribed");
		this.subscription = subscription;
		subscription.request(SUB_REQ);

	}

	public void onNext(Tweet tweet) {
		takeRest();

		logger.info("---------------------------------------------------------------");
		logger.info(tweet.toString());
		logger.info("---------------------------------------------------------------");
		subscription.request(SUB_REQ);
	}

	public void onError(Throwable throwable) {
		logger.log(Level.SEVERE, "An error occured", throwable);
	}

	public void onComplete() {
		logger.info("Done!");
	}

	private void takeRest() {
		try {
			Thread.sleep(SLEEP);
		} catch (InterruptedException e) {
			logger.log(Level.SEVERE, "An error has occured: {}", e);
		}
	}

	public String getId() {
		return id;
	}
}
