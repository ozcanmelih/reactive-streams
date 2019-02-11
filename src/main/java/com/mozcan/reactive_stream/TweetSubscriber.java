package com.mozcan.reactive_stream;

import java.util.UUID;
import java.util.concurrent.Flow;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.mozcan.reactive_stream.model.Tweet;

import twitter4j.Status;

public class TweetSubscriber implements Subscriber<Status> {

	private Logger logger = Logger.getLogger(TweetPublisher.class.getName());

	private final String id = UUID.randomUUID().toString();
	private Flow.Subscription subscription;
	private static final int SUB_REQ = 5;
	private static final int SLEEP = 1000;

	@Override
	public void onSubscribe(Subscription subscription) {
		logger.info("subscriber: " + id + " ==> Subscribed");
		this.subscription = subscription;
		subscription.request(SUB_REQ);
	}

	@Override
	public void onNext(Status status) {
		try {
			Thread.sleep(SLEEP);
		} catch (InterruptedException e) {
			logger.log(Level.SEVERE, "An error has occured: {}", e);
		}

		Tweet t = new Tweet(status.getUser().getScreenName(), status.getText(), status.getCreatedAt());
		logger.info("---------------------------------------------------------------");
		logger.info(t.toString());
		logger.info("---------------------------------------------------------------");
		subscription.request(SUB_REQ);
	}

	@Override
	public void onError(Throwable throwable) {
		logger.log(Level.SEVERE, "An error occured", throwable);
	}

	@Override
	public void onComplete() {
		logger.info("Done!");
	}

	public String getId() {
		return id;
	}
}
