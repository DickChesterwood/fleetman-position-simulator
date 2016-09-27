package com.virtualpairprogrammers;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import org.springframework.jms.UncategorizedJmsException;
import org.springframework.jms.core.JmsTemplate;

/**
 * A callable (so we can invoke in on a executor and join on it) that sends messages
 * to a queue periodically - represeting the journey of a delivery vehicle.
 */
public class Journey implements Callable<Object> 
{
	private List<String> positions;
	private String vehicleName;
	private JmsTemplate jmsTemplate;

	public Journey(String vehicleName, List<String> positions, JmsTemplate jmsTemplate) 
	{
		this.positions = Collections.unmodifiableList(positions);
		this.vehicleName = vehicleName;
		this.jmsTemplate = jmsTemplate;
	}

	@Override
	public Object call() throws InterruptedException  
	{
		for (String nextReport: this.positions)
		{
			String[] data = nextReport.split("\"");
			String lat = data[1];
			String longitude = data[3];

			// Spring will convert a HashMap into a MapMessage using the default MessageConverter.
			HashMap<String,String> positionMessage = new HashMap<>();
			positionMessage.put("vehicle", vehicleName);
			positionMessage.put("lat", lat);
			positionMessage.put("long", longitude);
			positionMessage.put("time", new java.util.Date().toString());

			sendToQueue(positionMessage);

			// We have an element of randomness to help the queue be nicely 
			// distributed
			delay(Math.random() * 200 + 200);
		}
		System.out.println(vehicleName + " has now completed its journey. Having a tea break.");
		return null;
	}

	/**
	 * Sends a message to the position queue - we've hardcoded this in at present - of course
	 * this needs to be fixed on the course!
	 * @param positionMessage
	 * @throws InterruptedException 
	 */
	private void sendToQueue(Map<String, String> positionMessage) throws InterruptedException {
		boolean messageNotSent = true;
		while (messageNotSent)
		{
			// broadcast this report
			try
			{
				jmsTemplate.convertAndSend("positionQueue",positionMessage);
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

	private static void delay(double d) throws InterruptedException {
		Thread.sleep((long) d);
	}


}
