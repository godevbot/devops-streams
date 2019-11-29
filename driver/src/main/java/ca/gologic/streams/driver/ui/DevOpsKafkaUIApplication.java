package ca.gologic.streams.driver.ui;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;

import ca.gologic.devops.streams.TestServiceRequestProcessor;
import ca.gologic.devops.streams.TestServiceResponseProcessor;
import ca.gologic.devops.streams.utils.TestUtils;
import ca.gologic.streams.application.ApplicationServiceRequestProcessor;
import ca.gologic.streams.application.ApplicationServiceResponseProcessor;
import ca.gologic.streams.application.utils.ApplicationUtils;
import ca.gologic.streams.database.DatabaseServiceAuditProcessor;
import ca.gologic.streams.database.DatabaseServiceRequestProcessor;
import ca.gologic.streams.database.DatabaseServiceResponseProcessor;
import ca.gologic.streams.database.utils.DatabaseUtils;
import ca.gologic.streams.driver.utils.DriverUtils;
import ca.gologic.streams.schema.utils.StreamProcessorThread;

@SpringBootApplication
@Controller
public class DevOpsKafkaUIApplication {

  private static List<StreamProcessorThread> processorThreads = new ArrayList<StreamProcessorThread>();
  private static List<Thread> threads = new ArrayList<Thread>(); 
  
  public static void main(String[] args) throws ParseException {
	  
	// create Options object
	Options options = new Options();
	
	// add t option
	options.addOption( new Option("b", "broker", true, "Kafka broker address. Example: kafka.gologic.ca:29092") );
	options.addOption( new Option("s", "schema-registry", true, "Kafka broker public url. Example: http://kafka.gologic.ca:8081") );
	options.addOption( new Option("m", "modules", true, "Modules to load. Example with all possible values: ui,database,application,test") );
	
	CommandLineParser parser = (CommandLineParser) new DefaultParser();
	CommandLine cmd = parser.parse( options, args);
	
	if( !cmd.hasOption("broker") ) {
		System.out.println( "Please specify --broker argument in the form of: kafka.gologic.ca:29092");
		System.exit(1);
	}

	if( !cmd.getOptionValue("broker").equalsIgnoreCase("default") )
	{
		if( !cmd.hasOption("schema-registry") ) {
			System.out.println( "Please specify --schema-registry argument in the form of: http://kafka.gologic.ca:8081");
			System.exit(1);
		}
		
		DriverUtils.BOOTSTRAP_SERVERS_CONFIG = cmd.getOptionValue("broker");
		DriverUtils.SCHEMA_REGISTRY_URL_CONFIG = cmd.getOptionValue("schema-registry");
		
		System.out.println("gologic-streams bootstrap: broker setted to " + DriverUtils.BOOTSTRAP_SERVERS_CONFIG);
		System.out.println("gologic-streams bootstrap: schema-registry setted to " + DriverUtils.SCHEMA_REGISTRY_URL_CONFIG);
	}
	
	if( !cmd.hasOption("modules")) {
		System.out.println("gologic-streams bootstrap: Loading all streams modules");
	}
	
	if( !cmd.hasOption("modules") || cmd.getOptionValue("modules").contains("ui") ) {
		System.out.println("gologic-streams bootstrap: Loading stream ui module");
		SpringApplication.run(DevOpsKafkaUIApplication.class, args);
	}

	if( !cmd.hasOption("modules") || cmd.getOptionValue("modules").contains("database") ){
		System.out.println("gologic-streams bootstrap: Loading stream database module");
		
		DatabaseUtils.BOOTSTRAP_SERVERS_CONFIG = DriverUtils.BOOTSTRAP_SERVERS_CONFIG;
		DatabaseUtils.SCHEMA_REGISTRY_URL_CONFIG = DriverUtils.SCHEMA_REGISTRY_URL_CONFIG;
		
		processorThreads.add( new DatabaseServiceRequestProcessor() );
    	processorThreads.add( new DatabaseServiceAuditProcessor() );
    	processorThreads.add( new DatabaseServiceResponseProcessor() );
	}
	
	if( !cmd.hasOption("modules") || cmd.getOptionValue("modules").contains("application") ){
		System.out.println("gologic-streams bootstrap: Loading stream application module");
		
		ApplicationUtils.BOOTSTRAP_SERVERS_CONFIG = DriverUtils.BOOTSTRAP_SERVERS_CONFIG;
		ApplicationUtils.SCHEMA_REGISTRY_URL_CONFIG = DriverUtils.SCHEMA_REGISTRY_URL_CONFIG;

		processorThreads.add( new ApplicationServiceRequestProcessor() );
		processorThreads.add( new ApplicationServiceResponseProcessor() );
	}
	
	if( !cmd.hasOption("modules") || cmd.getOptionValue("modules").contains("test") ){
		System.out.println("gologic-streams bootstrap: Loading stream test module");
		
		TestUtils.BOOTSTRAP_SERVERS_CONFIG = DriverUtils.BOOTSTRAP_SERVERS_CONFIG;
		TestUtils.SCHEMA_REGISTRY_URL_CONFIG = DriverUtils.SCHEMA_REGISTRY_URL_CONFIG;

		processorThreads.add( new TestServiceRequestProcessor() );
		processorThreads.add( new TestServiceResponseProcessor() );
	}

    for (Iterator<StreamProcessorThread> iterator = processorThreads.iterator(); iterator.hasNext();) {
		StreamProcessorThread streamProcessorThread = (StreamProcessorThread) iterator.next();
		
		Thread thread = new Thread(streamProcessorThread);
		threads.add(thread);
		thread.start();
	}
    
    String userMsg = "Type \"exit\" or \"e\" followed by [ENTER] to close processors and exit cleanly.\nUse [CTRL-C] to force shutdown.";
    String userInput = "";
	try {
		userInput = DriverUtils.readInput( userMsg );
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	
	while( ! userInput.equalsIgnoreCase("exit")  && !userInput.equalsIgnoreCase("e")){
    	try {
			userInput = DriverUtils.readInput( userMsg );
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }

    
    for (Iterator<StreamProcessorThread> iterator = processorThreads.iterator(); iterator.hasNext();) {
		StreamProcessorThread streamProcessorThread = (StreamProcessorThread) iterator.next();
		streamProcessorThread.stop();
	}

    for (Iterator<Thread> iterator = threads.iterator(); iterator.hasNext();) {
    	Thread streamProcessorThread = (Thread) iterator.next();
		try {
			streamProcessorThread.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	System.exit(0);
    
  }
  
  @GetMapping("/")
  public String index() {
    return "index";
  }
}
