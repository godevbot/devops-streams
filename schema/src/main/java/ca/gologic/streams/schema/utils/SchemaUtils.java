package ca.gologic.streams.schema.utils;

import java.io.BufferedReader;
import java.io.Console;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Random;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.reflect.ReflectData;
import org.apache.commons.beanutils.PropertyUtils;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.opencsv.bean.CsvToBeanBuilder;

import ca.gologic.streams.schema.ServiceEvent;
import ca.gologic.streams.schema.ServiceEventVO;

public class SchemaUtils {
	
	private static Logger LOG = LoggerFactory.getLogger(SchemaUtils.class);
	
	public static enum DevopsCsvEvents { 
		  Services,
		  Artifacts
		}
 
	public static String BOOTSTRAP_SERVERS_CONFIG = "ocean.gologic.ca:29092";
	public static String SCHEMA_REGISTRY_URL_CONFIG = "http://ocean.gologic.ca:8081";
	private static Random random = new Random();
	
	private static List<ServiceEventVO> serviceEventList;

	
	public static List<ServiceEventVO> getServiceEventList() {
		return serviceEventList;
	}

	public static void setServiceEventList(List<ServiceEventVO> serviceEventList) {
		SchemaUtils.serviceEventList = serviceEventList;
	}


	public static ServiceEvent getServiceEvent( ServiceEventVO serviceEventVO) 
	{
		ServiceEvent serviceEvent = null;
		try {
			serviceEvent = ServiceEvent.newBuilder().build();
			List<Field> fields = serviceEvent.getSchema().getFields();
			for (Iterator<Field> iterator = fields.iterator(); iterator.hasNext();) {
				Field field = (Field) iterator.next();
				Object value = PropertyUtils.getProperty(serviceEventVO, field.name());
				PropertyUtils.setProperty(serviceEvent, field.name(), value);
			}
		} catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
			LOG.error("Cannot reflect serviceEvetVO", e);
		}		
		return serviceEvent;
	}

	public static ServiceEventVO getServiceEventVO( ServiceEvent serviceEvent ) 
	{
		ServiceEventVO serviceEventVO = null;
		try {
			serviceEventVO = new ServiceEventVO();
			List<Field> fields = serviceEvent.getSchema().getFields();
			for (Iterator<Field> iterator = fields.iterator(); iterator.hasNext();) {
				Field field = (Field) iterator.next();
				Object value = PropertyUtils.getProperty(serviceEvent, field.name());
				if( field.schema().getType() == Schema.Type.STRING ){
					PropertyUtils.setProperty(serviceEventVO, field.name(), value.toString());
				}else
				{
					PropertyUtils.setProperty(serviceEventVO, field.name(), value);
				}
			}
		} catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
			LOG.error("Cannot reflect serviceEvetVO", e);
		}		
		return serviceEventVO;
	}
	

	private static BufferedReader systemInBfr = new BufferedReader( new InputStreamReader(System.in));
	
	public static String readInput(String message) throws IOException
	{
		Console cons = System.console();
		String line = null;

		if( cons != null )
		{
			line = cons.readLine("%s", message + ": ");
		}else
		{
			System.out.println(message + ": ");
			line = systemInBfr.readLine();			
		}
		return line;
	}
  
    public static void main(String[] args) throws IOException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
    	SchemaUtils.downloadEventsCSV(DevopsCsvEvents.Services);
    	SchemaUtils.downloadEventsCSV(DevopsCsvEvents.Artifacts);
    	SchemaUtils.loadEventsCSV(DevopsCsvEvents.Services);
    	SchemaUtils.loadEventsCSV(DevopsCsvEvents.Artifacts);
    	SchemaUtils.generateEventsSchema();
	}
    
    public static void generateEventsSchema() throws IOException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
    	String userDirectory = Paths.get("")
    		        .toAbsolutePath()
    		        .toString();
    	Schema schema = ReflectData.get().getSchema(ServiceEventVO.class);
    	writeStringToFile( "ServiceEvent", schema.toString(true),  "/src/main/resources/avro/ca/gologic/streams/ServiceEvent.avsc", userDirectory);
    }
    
    public static void writeStringToFile(String name, String str, String fileName, String directory) throws IOException, IllegalAccessException, InvocationTargetException, NoSuchMethodException
    {
        Gson gson =  new GsonBuilder().setPrettyPrinting().create();
        JsonObject object = gson.fromJson(str, JsonObject.class);
        object.addProperty("name", name);
        object.addProperty("namespace", "ca.gologic.streams.schema");
        str = gson.toJson(object);
        
        FileWriter fileWriter = new FileWriter(directory + fileName);
        PrintWriter printWriter = new PrintWriter(fileWriter);
        printWriter.print(str);
        printWriter.close();
        
    }
    
	public static void downloadEventsCSV(SchemaUtils.DevopsCsvEvents devopsCsvEvents) throws IOException {
		int CONNECT_TIMEOUT = 5000;
		int READ_TIMEOUT = 5000;
		
		String userDirectory = Paths.get("").toAbsolutePath().toString();
		String fileName = userDirectory + "/src/main/resources/data/DevOpsEvents-" + devopsCsvEvents + ".csv";

		String docUrl;
		if( DevopsCsvEvents.Services == devopsCsvEvents )
			docUrl = "https://docs.google.com/spreadsheets/u/5/d/1Op-wGKFr-zkBEdJOrFmKh0VE63yNedLYj0RnqZVvEL8/export?format=csv&id=1Op-wGKFr-zkBEdJOrFmKh0VE63yNedLYj0RnqZVvEL8&gid=0";
		else
			docUrl = "https://docs.google.com/spreadsheets/u/5/d/1Op-wGKFr-zkBEdJOrFmKh0VE63yNedLYj0RnqZVvEL8/export?format=csv&id=1Op-wGKFr-zkBEdJOrFmKh0VE63yNedLYj0RnqZVvEL8&gid=990761045";

		URL url = new URL(docUrl);
		File csv = new File(fileName);
		FileUtils.copyURLToFile(url, csv, CONNECT_TIMEOUT, READ_TIMEOUT);
	}
      
    public static void loadEventsCSV(SchemaUtils.DevopsCsvEvents devopsCsvEvents) {
    	
    	InputStream inputStream = SchemaUtils.class.getClassLoader().getResourceAsStream("data/DevOpsEvents-" + devopsCsvEvents + ".csv");
    	
    	if( DevopsCsvEvents.Services == devopsCsvEvents && serviceEventList == null) {
    		serviceEventList = new CsvToBeanBuilder<ServiceEventVO>(new InputStreamReader(inputStream)).withIgnoreQuotations(true)
                    .withSeparator(',')
                    .withType(ServiceEventVO.class)
                    .build()
                    .parse();
        	
        	for (ListIterator<ServiceEventVO> iterator = serviceEventList.listIterator(); iterator.hasNext();) {
    			ServiceEventVO devopsEventService = iterator.next();
    			System.out.println( iterator.nextIndex() + " : " + devopsEventService );
    		}  		
    	}
    }
    
    @SuppressWarnings({ "rawtypes", "unchecked" })
	public static void tryResetOffset(Consumer consumer, String topic)
    {
    	for( int i=1; i<=10; i++)
    	{
    		try {
    			Map<TopicPartition, OffsetAndMetadata> commitMessage = new HashMap<>();
    			commitMessage.put(new TopicPartition(topic, 0), new OffsetAndMetadata(0));
    			consumer.commitSync(commitMessage, Duration.ofSeconds(1));	
			} catch (Exception e) {
				LOG.info("Retries " + i +"/10");
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e1) {
					e1.printStackTrace();
				}
			}
    	}   
    }
    
    public static String getNextVersion(String version, String env)
    {
    	String nextVersion = version;
		switch ( env ){
			case "staging":
				nextVersion = version.replaceAll("-snapshot", "-rc");
				break;
			case "production":
				nextVersion = version.replaceAll("-snapshot", "").replaceAll("-rc", "");
				break;
			case "dev":
				nextVersion = version.replaceAll("-snapshot", "").replaceAll("-rc", "") + "-snapshot";
			default:
				break;
		}
		return nextVersion;
    }
    
    public static String getNextEnv(String env)
    {
    	String nextEnv = "dev";
		switch ( env ){
			case "dev":
				nextEnv = "staging";
				break;
			case "staging":
				nextEnv = "production";
				break;
			default:
				break;
		}
		return nextEnv;
    }
    
    public static String getNextMajorVersionString(String version)
    {
    	return getMajorVersionString( getMajorVersion(version) + 1 );
    }
    
    public static String getMajorVersionString(int dVersion)
    {
  	  return String.valueOf( ((int)dVersion) );
    }
    
    public static int getMajorVersion(String version)
    {
  	  String majorVersion = getNextVersion( version, "production" );
  	  int iVersion = 1;
  	  
  	  try {
  		iVersion = (int)Double.parseDouble( majorVersion );
  	  }catch(NumberFormatException e)
  	  {
  		  e.printStackTrace();
  	  }
  	
  	  return iVersion;
    }
    
    public static void sleep()
    {
    	try {
			Thread.sleep( random.nextInt(StreamProcessorThread.DEFAULT_SLEEP));
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
}
