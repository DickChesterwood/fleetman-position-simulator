package com.virtualpairprogrammers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.jms.UncategorizedJmsException;
import org.springframework.jms.core.JmsTemplate;

@SpringBootApplication
public class PositionsimulatorApplication {

	private static Map<String, List<String>> reports;

	public static void main(String[] args) throws IOException 
	{
		setUpAllData();

		try (ConfigurableApplicationContext ctx = SpringApplication.run(PositionsimulatorApplication.class, args)) 
		{
			JmsTemplate template = ctx.getBean(JmsTemplate.class);
			while (!reports.isEmpty())
			{
				for (String vehicleName : reports.keySet())
				{
					// get a report from each - if the reports are exhausted, remove that vehicle.
					List<String> thisVehiclesReports = reports.get(vehicleName);
					String report = thisVehiclesReports.remove(0);
					
					if (thisVehiclesReports.isEmpty()) 
					{
						reports.remove(vehicleName);
						System.out.println(vehicleName +" has now completed its journey. Having a tea break.");
						if (reports.isEmpty()) {
							System.out.println("All vehicles back at the start - get moving again");
							setUpAllData();
						}
					}
					
					String[] data = report.split("\"");
					String lat = data[1];
					String longitude = data[3];
									
					// Spring will convert a HashMap into a MapMessage using the default MessageConverter.
					HashMap<String,String> positionMessage = new HashMap<>();
					positionMessage.put("vehicle", vehicleName);
					positionMessage.put("lat", lat);
					positionMessage.put("long", longitude);
					positionMessage.put("time", new java.util.Date().toString());

					boolean messageNotSent = true;
					while (messageNotSent)
					{
						// broadcast this report
						try
						{
							template.convertAndSend("positionQueue",positionMessage);
							messageNotSent = false;
						}
						catch (UncategorizedJmsException e)
						{
							// we are going to assume that this is due to downtime - back off and go again
							System.out.println("Queue unavailable - backing off 5000ms before retry");
							delay(5000);
						}
					}
				}				
				delay(300);
			}
		}
	}

	private static void setUpAllData() throws IOException
	{
		reports = new ConcurrentHashMap<>();
		PathMatchingResourcePatternResolver path = new PathMatchingResourcePatternResolver();

		for (Resource nextFile : path.getResources("tracks/*"))
		{
			String vehicleName = nextFile.getFilename();

			try (Scanner sc = new Scanner(nextFile.getFile()))
			{
				List<String> thisVehicleReports = new ArrayList<>();
				while (sc.hasNextLine())
				{
					String nextReport = sc.nextLine();
					thisVehicleReports.add(nextReport);
				}
				reports.put(vehicleName,thisVehicleReports);
			}
		}
	}

	private static void delay(int millis) {
		try 
		{
			Thread.sleep(millis);
		}
		catch (InterruptedException e1) 
		{
		}
	}
}

