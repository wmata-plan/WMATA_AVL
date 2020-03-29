here's a copy paste from our internal knowledge base:
The filename uses the standard Clever naming convention
Rawnav – 5 digits for the bus ID – date string
So for instance:
Rawnav02515121108 is
Bus 2515
Date = 11/08/12
Inside the rawnav you’ll see the following on top:
/ 14:53:30 BWRawNav Collection Module was STARTED 07.72 DB=S0000313
cal,0,0
cal,15128,110
/ 14:55:38 Buswares is now using route zero
It shows the database version (S0000313) the buswares version (07.72) The time which it started the log file (14:53:30) and some calibration values
You’ll see a few tags in between the main lines of data:
PO0016X,2515,11/07/12,14:55:39,44988,05280
This is saying it’s running pullout 0016X, bus ID is 2515, the date is 11/07/12, the time is 14:55:39.  I’m not too sure what the last two lines are.
The main set of data is located in rows that look like this:
38.842957,-77.054228,104,C,S,000000,0000,08,   ,9,38.842930,-77.054210
1st column : Latitude RMC – corrected gps latitude
2nd column: Longitude RMC – corrected gps longitude
3rd column: Heading – in degrees
4th column: Door state – C for closed, O for open
5th column: Vehicle state – S for stopped, M for moving
6th column: Odometer – in feet
7th column: seconds – past start of trip
8th column: satellite count
9th column : Enter/exit stop window and feet differential
10th column: ?
11th column – Latitude GGA – raw gps (only different in ivn2s)
12th column – Longitude GGA – raw gps (only different in ivn2s)
In looking at this data, there seems to be a min threshold in both distance difference and or time before it records a position.  This can be every second or every few seconds depending if the bus was in motion or not.  It is a lot of data, but it is probably the largest set of navigation data collected by the vehicle. 

