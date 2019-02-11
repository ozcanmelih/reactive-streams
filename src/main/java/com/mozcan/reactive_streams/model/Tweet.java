package com.mozcan.reactive_streams.model;

import java.util.Date;

public class Tweet {

	private long id;
	private String user;
	private String text;
	private Date date;

	public Tweet(long id, String user, String text, Date date) {
		this.id = id;
		this.user = user;
		this.text = text;
		this.date = date;
	}

	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public String getText() {
		return text;
	}

	public void setText(String text) {
		this.text = text;
	}

	public Date getDate() {
		return date;
	}

	public void setDate(Date date) {
		this.date = date;
	}

	@Override
	public String toString() {
		return "@" + user + " tweeted: '" + text + "' [" + date + "]";
	}
}
