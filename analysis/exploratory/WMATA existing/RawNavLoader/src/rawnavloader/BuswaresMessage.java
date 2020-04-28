package rawnavloader;

/**
*
* @author E032409
*/

import java.sql.Connection;
import java.sql.PreparedStatement;

public class BuswaresMessage {
	String busID;
	String svcDate;
	String theTime;
	String message;
	boolean saved = false;
	
	String insertQuery = 
         "insert into rawnav_bw_message (id, svc_date, the_time, bus_id, message)" +
         "   values (rawnav_bw_message_seq.NEXTVAL, ?, ?, ?, ?)";
	
	public BuswaresMessage(String line, String svcDate, String busID) {
		/* Sample lines:
		 	/ 10:12:02 Buswares is now using route zero 
		 	/ 10:12:02 Buswares is Shutting down WaitResult SHUTDOWN,148791
		 	/ 14:53:21 BWRawNav Collection Module was STARTED 9.14.0.287 DB=S0000581M01 
		 */
		
		try {
			this.theTime = line.substring(2,10);
			this.message = line.substring(11);
			this.svcDate = svcDate;
			this.busID = busID;
			
		} catch (Exception e) {
			System.out.println("Could not read the following Buswaresmessage:");
			System.out.println(line);
			e.printStackTrace();
		}
	}
	
	public boolean save(Connection c) {
		if (this.saved = true) {return false;}
		boolean result = false;
		
		try {
			PreparedStatement statement = c.prepareStatement(this.insertQuery);
			
			statement.setString(1, this.svcDate);
			statement.setString(2, this.theTime);
			statement.setString(3, this.busID);
			statement.setString(4, this.message);
			
			if (statement.executeUpdate() > 0) {
				result = true;
			}
			statement.close();
			
		} catch (Exception e) {
			System.out.println(e);
			e.printStackTrace();
			System.out.println("Failed to save BuswaresMessage to DB.");
		}
		this.saved = true;
		return result;
	}
}
