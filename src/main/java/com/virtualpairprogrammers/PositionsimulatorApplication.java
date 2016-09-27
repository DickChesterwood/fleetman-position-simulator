package com.virtualpairprogrammers;

import java.io.IOException;
import java.util.Scanner;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * Written for the Microservices course, this is a toy application which simulates the progress
 * of vehicles on a delivery route. The program reads from one or more text files containing
 * a list of lat/long positions (these can be created from .gpx files or similar).
 * 
 * Messages are then sent on to a queue (currently hardcoded as positionQueue - to be fixed on 
 * the course!)
 * 
 * Intended for use on the training videos, questions to contact@virtualpairprogrammers.com
 * 
 * @author Richard Chesterwood
 */
@SpringBootApplication
public class PositionsimulatorApplication {

	public static void main(String[] args) throws IOException, InterruptedException 
	{
		try(ConfigurableApplicationContext ctx = SpringApplication.run(PositionsimulatorApplication.class))
		{
			final JourneySimulator simulator = ctx.getBean(JourneySimulator.class);

			Thread mainThread = new Thread(simulator);
			mainThread.start();

			System.out.println("Press return to terminate the simulation.");
			Scanner sc = new Scanner(System.in);
			sc.nextLine();
			sc.close();

			// try with resources will automatically close the container.
			simulator.finish();
		}
		
	}

}

