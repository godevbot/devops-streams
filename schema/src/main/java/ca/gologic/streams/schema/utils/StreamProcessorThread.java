package ca.gologic.streams.schema.utils;

public interface StreamProcessorThread extends Runnable {
	
	public int DEFAULT_SLEEP = 3000;
	
	public void stop();
}
