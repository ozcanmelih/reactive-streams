package com.mozcan.reactive_stream;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.SubmissionPublisher;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.mozcan.reactive_stream.config.Messages;

import twitter4j.Query;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.conf.ConfigurationBuilder;

public class TweetPublisher implements Publisher<Status> {

	private Logger logger = Logger.getLogger(TweetPublisher.class.getName());

	private static final int CORE_POOL_SIZE = 1;
	private static final int NB_THREADS = 1;
	private static final int INITIAL_DELAY = 1;
	private static final int DELAY = 5;
	private static final int MAX_SECONDS_TO_KEEP_IT_WHEN_NO_SPACE = 2;
	private static final int NUMBER_OF_TWEETS = 100;

	private Query twitterQuery;
	private Twitter twitter;

	private final ExecutorService EXECUTOR = Executors.newFixedThreadPool(NB_THREADS);
	private SubmissionPublisher<Status> publisher = new SubmissionPublisher<Status>(EXECUTOR, NUMBER_OF_TWEETS);

	private Set<Long> tweetCache = new HashSet<Long>();

	public TweetPublisher() {
		setup();
		getTweets();
	}

	@Override
	public void subscribe(Subscriber<? super Status> subscriber) {
		publisher.subscribe(subscriber);
	}

	private void setup() {
		twitterQuery = new Query(Messages.getAsString("query"));
		twitterQuery.setResultType(Query.ResultType.mixed);
		ConfigurationBuilder cb = new ConfigurationBuilder();
		cb.setDebugEnabled(true).setOAuthConsumerKey(Messages.getAsString("consumerKey"))
				.setOAuthConsumerSecret(Messages.getAsString("consumerSecret"))
				.setOAuthAccessToken(Messages.getAsString("accessToken"))
				.setOAuthAccessTokenSecret(Messages.getAsString("accessTokenSecret"));
		TwitterFactory tf = new TwitterFactory(cb.build());
		twitter = tf.getInstance();
	}

	private void getTweets() {
		ScheduledExecutorService executor = Executors.newScheduledThreadPool(CORE_POOL_SIZE);
		Runnable tweets = () -> {
			try {
				twitter.search(twitterQuery).getTweets().stream().filter(status -> {
					return !tweetCache.contains(status.getId());
				}).forEach(status -> {
					tweetCache.add(status.getId());

//					sb.submit(status);
					publisher.offer(status, MAX_SECONDS_TO_KEEP_IT_WHEN_NO_SPACE, TimeUnit.SECONDS,
							(subscriber, stat) -> {
								subscriber.onError(new RuntimeException("Subscriber "
										+ ((TweetSubscriber) subscriber).getId() + "! You are too slow getting tweets"
										+ " and we don't have more space for them! " + "I'll drop tweet: "
										+ stat.getId()));
								return false; // don't retry, we don't believe in second opportunities
							});
				});
			} catch (TwitterException e) {
				logger.log(Level.SEVERE, "AN error occured while fetching tweets");
				// close stream
				publisher.closeExceptionally(e);
			}
		};
		executor.scheduleWithFixedDelay(tweets, INITIAL_DELAY, DELAY, TimeUnit.SECONDS);
	}
}
