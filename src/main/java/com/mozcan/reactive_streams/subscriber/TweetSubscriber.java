package com.mozcan.reactive_streams.subscriber;

import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.mozcan.reactive_streams.model.Tweet;

public class TweetSubscriber implements Subscriber<Tweet> {

	private Logger logger = Logger.getLogger(getClass().getName());

	private String name;
	private int sleep;
	private int backPressure;

	private Subscription subscription;

	private AtomicInteger counter = new AtomicInteger();

	public TweetSubscriber(String id, int sleep, int backPressure) {
		this.name = id;
		this.sleep = sleep;
		this.backPressure = backPressure;
	}

	public void onSubscribe(Subscription subscription) {
		logger.info("[" + name + "] subscribed");
		this.subscription = subscription;
		subscription.request(backPressure);

	}

	public void onNext(Tweet tweet) {
		int totalCount = counter.incrementAndGet();

		logger.info("[" + name + "] " + tweet.toString());

		if (totalCount % backPressure == 0) {
			takeRest();
			subscription.request(backPressure);
		}
	}

	public void onError(Throwable throwable) {
		logger.log(Level.SEVERE, "An error occured", throwable);
	}

	public void onComplete() {
		logger.info("Completed!");
	}

	private void takeRest() {
		try {
			Thread.sleep(sleep);
		} catch (InterruptedException e) {
			logger.log(Level.SEVERE, "An error has occured: {}", e);
		}
	}

	public String getName() {
		return name;
	}
}
