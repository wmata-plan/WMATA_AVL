/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rawnavloader;

import java.sql.Connection;
import java.sql.PreparedStatement;

/**
 *
 * @author E011655
 */
public class ApcReading {
	// sample line 
	// 'apc,55,14,1,21,0,0'
	
    int x1, x2, x3, x4, x5, x6;
    
    GpsReading LastGpsR;
    boolean saved = false;
    String insertQuery = 
            "insert into rawnav_apc_reading (id, x1, x2, x3, x4, x5, x6, last_gps_reading_id)" +
         "   values (rawnav_apc_reading_seq.NEXTVAL, ?, ?, ?, ?, ?, ?, ?)";

    public ApcReading(String line, GpsReading LastGpsR) {
    	try {
    		String[] params = line.split(",");
    		
    		this.x1 = Integer.parseInt(params[1]);
    		this.x2 = Integer.parseInt(params[2]);
    		this.x3 = Integer.parseInt(params[3]);
    		this.x4 = Integer.parseInt(params[4]);
    		this.x5 = Integer.parseInt(params[5]);
    		this.x6 = Integer.parseInt(params[6]);
    		
    		this.LastGpsR = LastGpsR;
    		
    	} catch (Exception e) {
    		System.out.println("Failed to parse this APC line: " + line);
    	}
    }
    
    public boolean save(Connection c) {
    	if (this.saved) {return false;}
    	boolean result = false;
    	try {
    		PreparedStatement statement = c.prepareStatement(insertQuery);
    		
    		statement.setInt(1, x1);
    		statement.setInt(2, x2);
    		statement.setInt(3, x3);
    		statement.setInt(4, x4);
    		statement.setInt(5, x5);
    		statement.setInt(6, x6);
    		
    		statement.setInt(7, LastGpsR.id);
    		
    		if (statement.executeUpdate() > 0) {
    			result = true;
    		}
    		statement.close();
    		this.saved = true;
    		
    	} catch (Exception e) {
    		System.out.println(e);
    		System.out.println("Failed to save APC line");
    	}
    	
    	return result;
    }
}
