package simpledb.server;

import simpledb.remote.*;
import java.rmi.registry.*;

public class Startup {
   public static void main(String args[]) throws Exception {
	  
	  int gClock = 5;
	  try {
		  gClock = Integer.parseInt(args[1]);
	  } catch (Exception e) {
		  System.out.println("Could not parse an int from the second argument, using 5 instead.");
	  }
	  // configure and initialize the database
	  // If no command line argument was given for the number of rotation allowed by gClock,
	  // or no int is parsed from the input, the default value of 5 is used.
      SimpleDB.init(args[0], gClock);
      
      
      // create a registry specific for the server on the default port
      Registry reg = LocateRegistry.createRegistry(1099);
      
      // and post the server entry in it
      RemoteDriver d = new RemoteDriverImpl();
      reg.rebind("simpledb", d);
      
      System.out.println("database server ready");
   }
}
