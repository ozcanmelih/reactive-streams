package com.mozcan.reactive_streams.publisher;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.SubmissionPublisher;
import java.util.concurrent.TimeUnit;

import com.mozcan.reactive_streams.async.TweetQueryWorker;
import com.mozcan.reactive_streams.config.Constants;
import com.mozcan.reactive_streams.model.Tweet;
import com.mozcan.reactive_streams.subscriber.TweetSubscriber;

import twitter4j.Status;

public class TweetPublisher implements Publisher<Tweet> {

	private Executor executor = Executors.newFixedThreadPool(Constants.PUBLISHER_SCHEDULER_N_THREADS);
	private SubmissionPublisher<Tweet> publisher = new SubmissionPublisher<Tweet>(executor,
			Constants.PUBLISHER_MAX_BUFFER_CAPACITY);

	ScheduledExecutorService scheduledExecutor = Executors
			.newScheduledThreadPool(Constants.SCHEDULED_JOB_CORE_POOL_SIZE);

	private Set<Long> tweetCache = new HashSet<Long>();

	public TweetPublisher() {
		getTweets();
	}

	public void subscribe(Subscriber<? super Tweet> subscriber) {
		publisher.subscribe(subscriber);
	}

	public void offer(List<Status> tweets) {

		tweets.stream().filter(status -> {
			return !tweetCache.contains(status.getId());
		}).forEach(status -> {
			tweetCache.add(status.getId());
			Tweet tweet = createTweet(status);

			publisher.offer(tweet, Constants.MAX_SECONDS_TO_KEEP_IT_WHEN_NO_SPACE, TimeUnit.SECONDS,
					(subscriber, currentTweet) -> {
						subscriber.onError(new RuntimeException("[" + ((TweetSubscriber) subscriber).getName()
								+ "] is too slow getting tweets and buffer don't have more space for them! "
								+ "Tweet'll be droped: " + currentTweet.getId()));
						return false;
					});
		});
	}

	public void closeExceptionally(Throwable e) {
		publisher.closeExceptionally(e);
	}

	private void getTweets() {
		TweetQueryWorker worker = new TweetQueryWorker(this);
		scheduledExecutor.scheduleWithFixedDelay(worker, Constants.SCHEDULED_JOB_INITIAL_DELAY,
				Constants.SCHEDULED_JOB_DELAY, TimeUnit.SECONDS);
	}

	private Tweet createTweet(Status status) {
		return new Tweet(status.getId(), status.getUser().getScreenName(), status.getText(), status.getCreatedAt());
	}

	public void close() {
		scheduledExecutor.shutdown();
		while (!scheduledExecutor.isTerminated()) {
		}
		publisher.close();
	}
}
