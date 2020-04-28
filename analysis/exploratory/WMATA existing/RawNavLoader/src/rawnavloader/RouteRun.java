/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rawnavloader;

import java.util.LinkedList;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Iterator;

/**
 *
 * @author E011655
 */
public class RouteRun {
        String insertQuery = "insert into rawnav_route_run (id, route_pattern, bus_id, the_date, the_time, svc_date, corrected, invalid) "
        		+ "values (rawnav_route_run_seq.NEXTVAL, ?, ?, ?, ?, ?, ?, ?)";
        String routePattern;
        String busId;
        String theDate;
        String theTime;
        String unknown1;
        String unknown2;
        String svcDate;
        LocalDateTime startDateTime;
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("MM/dd/yy HH:mm:ss"); // param == theDate + svcDate
        boolean constructorCalled = false;
        int corrected = 0;
        int invalid = 0;
        
        int id; 
        
        LinkedList<GpsReading> gpsList;
  
        
        /* (non-Javadoc)
		 * @see java.lang.Object#hashCode()
		 */
		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((busId == null) ? 0 : busId.hashCode());
			result = prime * result + ((routePattern == null) ? 0 : routePattern.hashCode());
			result = prime * result + ((svcDate == null) ? 0 : svcDate.hashCode());
			result = prime * result + ((unknown1 == null) ? 0 : unknown1.hashCode());
			result = prime * result + ((unknown2 == null) ? 0 : unknown2.hashCode());
			return result;
		}

		/* (non-Javadoc)
		 * @see java.lang.Object#equals(java.lang.Object)
		 */
		@Override
		public boolean equals(Object obj) {
			if (this == obj) {
				return true;
			}
			if (obj == null) {
				return false;
			}
			if (!(obj instanceof RouteRun)) {
				return false;
			}
			RouteRun other = (RouteRun) obj;
			if (busId == null) {
				if (other.busId != null) {
					return false;
				}
			} else if (!busId.equals(other.busId)) {
				return false;
			}
			if (routePattern == null) {
				if (other.routePattern != null) {
					return false;
				}
			} else if (!routePattern.equals(other.routePattern)) {
				return false;
			}
			if (svcDate == null) {
				if (other.svcDate != null) {
					return false;
				}
			} else if (!svcDate.equals(other.svcDate)) {
				return false;
			}
			if (unknown1 == null) {
				if (other.unknown1 != null) {
					return false;
				}
			} else if (!unknown1.equals(other.unknown1)) {
				return false;
			}
			if (unknown2 == null) {
				if (other.unknown2 != null) {
					return false;
				}
			} else if (!unknown2.equals(other.unknown2)) {
				return false;
			}
			return true;
		}

	public RouteRun(String s, String svcDate) {
            this.constructorCalled = true;
            String[] params = s.split(",");
            
            routePattern = params[0].trim();
            busId = params[1];
            theDate = params[2];
            theTime = params[3];
            this.svcDate = svcDate;
            // TODO: add svcDate to DB?
            String dateTimeText = this.theDate + " " + theTime;
            this.startDateTime = LocalDateTime.parse(dateTimeText, this.formatter);
            
            unknown1 = params[4];
            unknown2 = params[5];
            
            gpsList = new LinkedList<GpsReading>();
            
        }
        
        public RouteRun() {
        }
        
        public void addGpsReading(GpsReading gps) {
            gpsList.add(gps);
        }
        
        public GpsReading getFirstGpsReading() {
        	if (this.gpsList.size()>0) {
        		return this.gpsList.getFirst();
        	}
        	return null;
        }
        
        public GpsReading getLastGpsReading() {
        	if (this.gpsList.size()>0) {
        		return this.gpsList.getLast();
        	}
        	return null;
        }
        
        public void setCorrected() {
        	this.corrected = 1;
        }
        
        public void setInvalid() {
        	this.invalid = 1;
        }
        
        public boolean save(Connection c) {
        	GpsReading last = this.getLastGpsReading();
        	if (last != null) {
        		int lastOdo = last.odoFeet;
        		if (lastOdo == 0) return false;
        	}
        	
        	if (!this.constructorCalled) {return false;}
        	
            boolean result = true;
            try {
                Connection conn = c;
                PreparedStatement stmt = c.prepareStatement(insertQuery);
                stmt.setString(1, routePattern);
                stmt.setString(2, busId);
                stmt.setString(3, theDate);
                stmt.setString(4, theTime);
                stmt.setString(5, this.svcDate);
                stmt.setInt(6, this.corrected);
                stmt.setInt(7, this.invalid);
                if(stmt.executeUpdate() > 0) {
                    PreparedStatement idQuery = c.prepareStatement("select rawnav_route_run_seq.currval from dual");
                    ResultSet rset = idQuery.executeQuery();
                    if(rset.next()) {
                        id = rset.getInt(1);
                    }
                    idQuery.close();
                }
                stmt.close();
                Iterator<GpsReading> itor = gpsList.iterator();
                while (itor.hasNext()) {
                    GpsReading g = (GpsReading)itor.next();
                    g.setRouteRunId(id);
                    g.save(c);
                }
                
                conn.commit();
            } catch (Exception e) {
                 System.out.println(e);
                 e.printStackTrace();
                result = false;
            }
            return result;
        }
        
        public static boolean exists(Connection c, String busId, String theDate) {
            boolean result = false;
            try {
                PreparedStatement stmt = c.prepareStatement("select count(*) the_count from rawnav_route_run where bus_id = ? and the_date = ?");
                stmt.setString(1, busId);
                stmt.setString(2, theDate);
                ResultSet rset = stmt.executeQuery();
                if(rset.next()) {
                    result = true;
                }
            } catch (Exception e) {
            	System.out.println(e);
            	e.printStackTrace();
            }    
            return result;
        }
    }
    
