package ca.gologic.streams.schema;

import com.opencsv.bean.CsvBindByName;

public class ServiceEventVO {

	//SRC: https://docs.google.com/spreadsheets/d/1RCGsEerN-m2z6Whba7NR2PFGgWuqGtJpMCwV_kCo7C8/edit?usp=sharing
	//EXPORT: /devops-stream/src/main/resources/data/DevOps Events - Services.csv
	//HEADERS: SUBJECT,SERVICE,ACTION,VERSION,ENV,PRODUCER,DOC
	
	@CsvBindByName(column = "SUBJECT" )
	private String subject;

	@CsvBindByName(column = "SERVICE" )
	private String service;
	
	@CsvBindByName(column = "ACTION" )
	private String action;
	
	@CsvBindByName(column = "VERSION" )
	private String version;
	
	@CsvBindByName(column = "ENV" )
	private String env;
	
	@CsvBindByName(column = "PRODUCER" )
	private String producer;
	
	@CsvBindByName(column = "STORY" )
	private String story;
	
	private String previousEventId;
	
	private String rootEventId;
	
	private Long count;
	
	public String getSubject() {
		return subject;
	}

	public void setSubject(String subject) {
		this.subject = subject;
	}

	public String getService() {
		return service;
	}

	public void setService(String service) {
		this.service = service;
	}

	public String getAction() {
		return action;
	}

	public void setAction(String action) {
		this.action = action;
	}

	public String getVersion() {
		return version;
	}

	public void setVersion(String version) {
		this.version = version;
	}

	public String getEnv() {
		return env;
	}

	public void setEnv(String env) {
		this.env = env;
	}

	public String getProducer() {
		return producer;
	}

	public void setProducer(String producer) {
		this.producer = producer;
	}

	public String getStory() {
		return story;
	}

	public void setStory(String story) {
		this.story = story;
	}

	public String getPreviousEventId() {
		return previousEventId;
	}

	public void setPreviousEventId(String previousEventId) {
		this.previousEventId = previousEventId;
	}

	public String getRootEventId() {
		return rootEventId;
	}

	public void setRootEventId(String rootEventId) {
		this.rootEventId = rootEventId;
	}
	
	public Long getCount() {
		return count;
	}

	public void setCount(Long count) {
		this.count = count;
	}

	@Override
	public String toString() {
		 return String.format("ServiceEventVO: %s,%s,%s,%s,%s" , subject, service, action, version, env);
	}
}
