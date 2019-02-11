package com.mozcan.reactive_streams;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.SubmissionPublisher;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.mozcan.reactive_streams.config.Messages;
import com.mozcan.reactive_streams.model.Tweet;

import twitter4j.Query;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.conf.ConfigurationBuilder;

public class TweetPublisher implements Publisher<Tweet> {

	private Logger logger = Logger.getLogger(getClass().getName());

	private static final int MAX_BUFFER_CAPACITY = 100;
	private static final int N_THREADS = 1;
	private static final int CORE_POOL_SIZE = 1;
	private static final int INITIAL_DELAY = 1;
	private static final int DELAY = 5;
	private static final int MAX_SECONDS_TO_KEEP_IT_WHEN_NO_SPACE = 2;

	private Twitter twitter;
	private Query twitterQuery;

	private Set<Long> tweetCache = new HashSet<Long>();

	private Executor executor = Executors.newFixedThreadPool(N_THREADS);
	private SubmissionPublisher<Tweet> publisher = new SubmissionPublisher<Tweet>(executor, MAX_BUFFER_CAPACITY);

	public TweetPublisher() {
		init();
		getTweets();
	}

	public void subscribe(Subscriber<? super Tweet> subscriber) {
		publisher.subscribe(subscriber);
	}

	private void init() {
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

		Runnable runner = () -> {
			try {
				twitter.search(twitterQuery).getTweets().stream().filter(status -> {
					return !tweetCache.contains(status.getId());
				}).forEach(status -> {
					tweetCache.add(status.getId());
					Tweet tweet = createTweet(status);

					publisher.offer(tweet, MAX_SECONDS_TO_KEEP_IT_WHEN_NO_SPACE, TimeUnit.SECONDS,
							(subscriber, currentTweet) -> {
								subscriber.onError(new RuntimeException("Subscriber "
										+ ((TweetSubscriber) subscriber).getId() + "! You are too slow getting tweets"
										+ " and we don't have more space for them! I'll drop tweet: "
										+ currentTweet.getId()));
								return false;
							});
				});
			} catch (TwitterException e) {
				logger.log(Level.SEVERE, "AN error occured while fetching tweets");
				publisher.closeExceptionally(e);
			}
		};

		executor.scheduleWithFixedDelay(runner, INITIAL_DELAY, DELAY, TimeUnit.SECONDS);
	}

	private Tweet createTweet(Status status) {
		return new Tweet(status.getId(), status.getUser().getScreenName(), status.getText(), status.getCreatedAt());
	}
}
