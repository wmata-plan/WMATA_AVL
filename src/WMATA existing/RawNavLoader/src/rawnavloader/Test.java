package rawnavloader;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
/**
 * Just a sandbox class
 * 
 * @author E032409
 *
 */
public class Test {
	public static void main(String[] args) {
		String svcDate = "03/14/18";
		String startTime = "09:42:01";
		String dateTimeText = svcDate + " " + startTime;
		// 03/13/18 09:42:00
		
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("MM/dd/yy HH:mm:ss");
		LocalDateTime parsed = LocalDateTime.parse(dateTimeText, formatter);
		
		System.out.println(parsed);
		
		System.out.println(distance(38.897045, -77.008338, 38.898180, -77.009033));

	}
	
	public static double distance(double lat1, double lon1, double lat2, double lon2) {
		// 38.897045, -77.008338, 38.898180, -77.009033
        final int R = 20898071; // Radius of the earth in feet

        double latDistance = Math.toRadians(lat2 - lat1);
        double lonDistance = Math.toRadians(lon2 - lon1);
        double a = Math.sin(latDistance / 2) * Math.sin(latDistance / 2)
                + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
                * Math.sin(lonDistance / 2) * Math.sin(lonDistance / 2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        double distance = R * c;

        distance = Math.pow(distance, 2);

        return Math.sqrt(distance);
    }

}
