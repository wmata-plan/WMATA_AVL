/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rawnavloader;

import java.sql.*;

/**
 *
 * @author E011655
 */
 public class GpsReading {
     	 public int id;
	 	 
         double latFixed;
         double lonFixed;
         int headingDeg;
         String doorState;
         String vehicleState;
         int odoFeet;
         int odoOffset;
         int timeSecondsPastStart;
         int timeOffset;
         int satCount;
         String stopWindowData;

         double latOrig;
         double lonOrig;
         
         int routeRunId;
         
         boolean saved = false;
         
         String insertQuery = 
                    "insert into rawnav_gps_reading (id, lat_fixed, lon_fixed, heading_Deg, door_state, vehicle_state, odo_feet, time_seconds, sat_count, stop_window_data, lat_orig, lon_orig, route_run_id, odo_offset, time_offset) " +
                 "   values (rawnav_gps_reading_seq.NEXTVAL, ?, ?, ?, ?, ?, ?, ?, ?, trim(?), ?, ?, ?, ?, ?)";
        
         ApcReading apcR = null;
         
      public GpsReading(String s) {
          
          try {
          String[] params = s.split(",");
          
          latFixed = Double.parseDouble(params[0]);
          lonFixed = Double.parseDouble(params[1]);
          
          headingDeg = Integer.parseInt(params[2]);
          
          doorState = params[3];
          vehicleState = params[4];
          
          odoFeet = Integer.parseInt(params[5]);
          
          timeSecondsPastStart = Integer.parseInt(params[6]);
          satCount = Integer.parseInt(params[7]);
          
          stopWindowData = params[8];
          
          latOrig = Double.parseDouble(params[10]);
          lonFixed = Double.parseDouble(params[11]);
          
          } catch (Exception e) {
              System.out.println("Failed to parse GpsReading: " + s);
              e.printStackTrace();
          }
          
          //System.out.println("Created GPS Reading");
      }          
      
      public void setRouteRunId(int l) {
        routeRunId = l;
      }
      
      public void setApcR(ApcReading apcR) {
    	  this.apcR = apcR;
      }
      
      public void setOdoOffset(int odoOffset) {
    	  this.odoOffset = odoOffset;
      }
      
      public void setTimeOffset(int timeOffset) {
    	  this.timeOffset = timeOffset;
      }
      
      /**
       * Calculate distance between this GpsReading and other GpsReading. 
       * Uses Haversine method as its base.
       * 
       * @returns distance in integer feet
       */
      public int distance_to(GpsReading other) {
          final int R = 20898071; // Radius of the earth in feet

          double latDistance = Math.toRadians(other.latFixed - this.latFixed);
          double lonDistance = Math.toRadians(other.lonFixed - this.lonFixed);
          double a = Math.sin(latDistance / 2) * Math.sin(latDistance / 2)
                  + Math.cos(Math.toRadians(this.latFixed)) * Math.cos(Math.toRadians(other.latFixed))
                  * Math.sin(lonDistance / 2) * Math.sin(lonDistance / 2);
          double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
          double distance = R * c;

          distance = Math.pow(distance, 2);

          return (int) Math.sqrt(distance);
      }
      
      public boolean save(Connection c) {
    	  	if (this.saved) {return false;}
    	  	boolean result = false;
            try {
                PreparedStatement stmt = c.prepareStatement(insertQuery);
 
                 stmt.setDouble(1, latFixed);
                 stmt.setDouble(2,  lonFixed);
                 stmt.setInt(3, headingDeg);
                 stmt.setString(4, doorState);
                 stmt.setString(5,  vehicleState);
                 stmt.setInt(6, odoFeet + odoOffset);
                 
                 
                 stmt.setInt(7,  timeSecondsPastStart + timeOffset);
                 stmt.setInt(8,  satCount);
                 stmt.setString(9, stopWindowData);

                 stmt.setDouble(10,  latOrig);
                 stmt.setDouble(11,  lonOrig);

                 stmt.setLong(12, routeRunId);
                 
                 stmt.setInt(13, odoOffset);
                 stmt.setInt(14, timeOffset);
                 
                 if(stmt.executeUpdate() > 0) {
                	 PreparedStatement idQuery = c.prepareStatement("select rawnav_gps_reading_seq.currval from dual");
                     ResultSet rset = idQuery.executeQuery();
                     if(rset.next()) {
                         id = rset.getInt(1);
                     }
                     idQuery.close();
                     
                    result = true;
                 }
                
                stmt.close();
                if (this.apcR != null) {
                	apcR.save(c);
                }
                this.saved = true;
                
            } catch (Exception e) {
                System.out.println(e);
                e.printStackTrace();
            }
            
            return result;
      
      }
    }

