package com.mozcan.reactive_streams.async;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.mozcan.reactive_streams.publisher.TweetPublisher;

import src.main.java.com.mozcan.reactive_streams.config.Messages;
import twitter4j.Query;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.conf.ConfigurationBuilder;

public class TweetQueryWorker implements Runnable {

	private Logger logger = Logger.getLogger(getClass().getName());

	private TweetPublisher publisher;

	private Twitter twitter;
	private Query twitterQuery;

	public TweetQueryWorker(TweetPublisher publisher) {
		this.publisher = publisher;

		initTwitterClient();
	}

	private void initTwitterClient() {
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

	@Override
	public void run() {
		try {
			List<Status> tweets = twitter.search(twitterQuery).getTweets();

			logger.info("Tweets fetched.");

			publisher.offer(tweets);
		} catch (TwitterException e) {
			logger.log(Level.SEVERE, "AN error occured while fetching tweets");
			publisher.closeExceptionally(e);
		}
	}
}
