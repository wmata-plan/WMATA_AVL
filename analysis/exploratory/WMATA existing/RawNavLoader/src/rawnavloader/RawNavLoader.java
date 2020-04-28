/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rawnavloader;
import java.util.zip.*;
import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.temporal.ChronoUnit;
import java.util.Scanner;
import java.util.ArrayList;


/**
 *
 * @author E011655
 */
public class RawNavLoader {
   
    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws Exception {
        // TODO code application logic here
        FileInputStream fios = null;
        ZipInputStream zipStream = null;
        
        String endOfRoute = "Buswares navigation reported end of route";
        String gpsSample = "47.763423,-96.623207,017,C,S,000000,0088,04,X-1,1,47.763410,-96.623207";
        
        /*
        File d = new File("\\\\L-600730\\w\\RawNavArchive\\032018");
        String fileNameFilter = "2517180315";
        
        File[] list = d.listFiles(new FilenameFilter() {
            public boolean accept(File d, String name) {
                return name.toLowerCase().contains(fileNameFilter);
            }
        });
        System.out.println("Got directory listing: " + list.length + " files matching " + fileNameFilter);
        */
        
        File [] list = loadFileList("S9", "01-MAY-19", "02-MAY-19", getConnection());
        
        System.out.println("Got buses matching criteria: " + list.length );
                
        int filesToProcess = 0;
        filesToProcess = list.length;
        //filesToProcess = 2;
        
        for(int i=0; i<filesToProcess; i++) {
            try {
                File f = list[i];
                fios = new FileInputStream(f);
                zipStream = new ZipInputStream(fios);
                ZipEntry entry;
                Connection conn = getConnection();

                RouteRun rr;
                while ((entry = zipStream.getNextEntry()) != null) {
                    System.out.println("Processing zip entry " + entry.toString());
//                    if (!entry.toString().contains(fileNameFilter)) {
//                        continue;
//                     }
                    // get busID and svcDate from file name:
                    String busID = Integer.toString(Integer.parseInt(entry.toString().substring(6, 11)));
                    
                    String yymmdd = entry.toString().substring(11, 17);
                    Integer dayInt = Integer.parseInt(yymmdd.substring(4,6))-1;
                    String dayStr;
                    if (dayInt <= 9) {
                    	dayStr = "0" + Integer.toString(dayInt);
                    } else {
                    	dayStr = Integer.toString(dayInt);
                    }
                    String svcDate = yymmdd.substring(2,4) + '/' + dayStr + '/' + yymmdd.substring(0,2);
                    
                    boolean onRoute = false;
                    int lines = 0;
                    int validRunCount = 0;
                    int routeEnds = 0;
                    int apcCount = 0;
                    int gpsCount = 0;
                    int msgCount = 0;
                    Scanner sc = new Scanner(zipStream);
                    
                    rr = new RouteRun();
                    RouteRun lastRun = null;
                    
                    GpsReading gpsR = null;
                    GpsReading lastGpsR = null;
                    
                    int odoOffset = 0;
                    int timeOffset = 0;
                    
                    while (sc.hasNextLine()) {
                        String line = sc.nextLine();
                        lines++;

                        if(line.startsWith(" ")) {
                             if (onRoute) { // If we didn't cleanly close out the previous run
                                rr.save(conn);
                                lastGpsR = null;
                                odoOffset = 0;
                                timeOffset = 0;
                             };
                             
                             rr = new RouteRun(line, svcDate);
                             // TODO: Implement lastRun = currentRun logic here.
                             // for example of where this is needed:
                             // see rawnav02531180315.txt:3967-75
                             
                             if (rr.equals(lastRun)) {
                            	 long startDiff = ChronoUnit.SECONDS.between(lastRun.startDateTime, rr.startDateTime);
                            	 if (startDiff<15*60) {
                            		 rr = lastRun; // throw out rr you just made
                            		 rr.setCorrected();
                            		 lastGpsR = gpsR;
                            	 } else {
                            		 // Flag route using this.invalid (?)
                            		 
                            		 // Here go other conditions for merging, as well as conditions for not merging, given rr = lastRun
                            		 // Not merging:
                            		 //		route is loop (?)
                            		 //		start point of lastR is the same as rr
                            	 }
                             }
                             // TODO:  Add logic here to destroy the "rr" and not set onRoute if it's not a route we're interested in.
                             lastRun = rr;
                             validRunCount++;
                             onRoute = true;
                        } else if (line.startsWith("PI") || line.startsWith("PO") || line.startsWith("DH")) {
                        	if (onRoute) {
                                rr.save(conn);
                                lastGpsR = null;
                                odoOffset = 0;
                                timeOffset = 0;
                             };
                        	onRoute = false;
                        } else if (line.startsWith("/ ")) {
                        	BuswaresMessage bwm = new BuswaresMessage(line, svcDate, busID);
                        	bwm.save(conn);
                        	msgCount++;
                        	if (line.contains(endOfRoute) && onRoute) {
                                routeEnds++;
                                rr.save(conn);
                                lastGpsR = null;
                                odoOffset = 0;
                                timeOffset = 0;
                                onRoute = false;
                            }
                        } else if (line.startsWith("apc") && onRoute && gpsR != null) {
                            ApcReading apcR = new ApcReading(line, gpsR);
                            // System.out.println("Line #:"+ lines);
                            // System.out.println(line);
                            gpsR.setApcR(apcR);
                            apcCount++;
                        } else if (onRoute && line.startsWith("3") && line.split(",").length == gpsSample.split(",").length)  {
                            gpsCount++;
                            gpsR = new GpsReading(line);
                            if (lastGpsR != null) {
                            	if (gpsR.odoFeet < lastGpsR.odoFeet) {
                            		// TODO This still doesn't happen correctly for corrected runs
                                	// check to see if current seconds > previous seconds
                                	// the odometer reset while on the same route run
                                	// TODO: Is this a good condition? Do we need a more rigorous test of odometer reset?
                                	// see rawnav02518180315.txt:6676 for an example
                                	odoOffset = lastGpsR.odoFeet;
                                }
                                if (gpsR.timeSecondsPastStart < lastGpsR.timeSecondsPastStart) {
                                	timeOffset = lastGpsR.timeSecondsPastStart;
                                }
//                            } else {
//                            	System.out.println("At this line Last GpsR is null");
//                            	System.out.println(line);
                            }
                            gpsR.setOdoOffset(odoOffset); // by default 0
                            gpsR.setTimeOffset(timeOffset); // by default 0
                            rr.addGpsReading(gpsR);
                            lastGpsR = gpsR;
                        }
                        
                    }
                    if (validRunCount > 0) {
                      System.out.println("  >>> " + entry + " Processed:  " + lines + " total lines, " + validRunCount + " runs, " + routeEnds + " run ends, "+ apcCount + " APC lines, " + gpsCount + " GPS lines, " + msgCount + " Buswares message lines");
                    } 
                   
                }
                
                conn.close();
                zipStream.close();
                fios.close();
            } catch (Exception e) {
                System.out.println(e);
            }
        }
             
    }

    private static Connection getConnection()   {
        Connection c = null;
        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");
            c = DriverManager.getConnection("jdbc:oracle:thin:db_location", "user", "pass");
            c.setAutoCommit(false);
        } catch (Exception e) {
            System.out.println("Connection failed.");
            e.printStackTrace();
        }
        return c;
    }
    
    private static File[] loadFileList(String route, String startDate, String endDate, Connection c) throws Exception {
        String rootFolder = "\\\\L-600730\\w\\RawNavArchive\\";
        String sqlStmt = "select distinct to_char(svc_date, 'MMYYYY') || '\\' || 'rawnav0' || bus_id || to_char(svc_date + 1, 'YYMMDD') || '.txt.zip' filename " +
            "from trace_bus_trip_v a " +
            //"where svc_date between ? and ? " +
            "where svc_date in( ?, ?) " +
            "  and route = ?";
        
        PreparedStatement stmt = c.prepareStatement(sqlStmt);
        stmt.setString(1, startDate);
        stmt.setString(2, endDate);
        stmt.setString(3, route);
        
        ArrayList<File> list = new ArrayList();
        
        ResultSet rs = stmt.executeQuery();
        
        while(rs.next()) {
            File f = new File(rootFolder + rs.getString("filename"));
            list.add(f);
        }
        
        File[] foo = new File[0];
        c.close();
        
        
        return list.toArray(foo);
        
        
        
        
    }
   
    
}
