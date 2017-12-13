# Name: Generate Transit Lines and Stops
# Author: Lin 
# Description: Create a feature dataset 

# Import system modules
import arcpy
from arcpy import env
from operator import itemgetter
import sqlize_csv
import hms
import sqlite3, os, operator, itertools, csv, re
from sets import Set


# enable file overwrite
arcpy.env.overwriteOutput = True

# GTFS directories
inGTFSdir = r'E:\Final Project\SFMTA_GTFS\google_transit'
outFD = r'E:\Final Project\Lins Workspace\Network Dataset\GTFSAnalysis.gdb'

# create dBASE tables into the workspace
#arcpy.CreateTable_management(workspace, "stops.dbf")

# Creating a spatial reference object
sr = arcpy.SpatialReference(4326)

# GTFS stop lat/lon are written in WGS1984 coordinates
WGSCoords = "GEOGCS['GCS_WGS_1984',DATUM['D_WGS_1984', \
SPHEROID['WGS_1984',6378137.0,298.257223563]], \
PRIMEM['Greenwich',0.0],UNIT['Degree',0.0174532925199433]]; \
-400 -400 1000000000;-100000 10000;-100000 10000; \
8.98315284119522E-09;0.001;0.001;IsHighPrecision"

FD = arcpy.management.CreateFeatureDataset(outFD, "nd", sr)

# (1) Generate Transit Stops Point Feature Class

#outStopFCName = "Stops"
#outStopFC = os.path.join(outFD,outStopFCName)

outStopsFC = arcpy.CreateFeatureclass_management(FD,"stops","POINT",spatial_reference=sr)

# read-in stops.txt
StopTable = inGTFSdir + "\stops.txt"

stopFields = [("stop_id","TEXT"),("stop_name","TEXT"),("stop_desc","TEXT"),("zone_id","TEXT"),("stop_url","TEXT")]

# adding fields to stops feature class
for field in stopFields:
    arcpy.management.AddField(outStopsFC, field[0], field[1])


# Initialize a dictionary of stop lat/lon (filled below)
    # {stop_id: <stop geometry object>} in the output coordinate system
stopGeomDict = {}

arcpy.AddMessage("Generating stops feature class.")

# feed data from stops.txt to the stops feature class
with arcpy.da.InsertCursor(outStopsFC, ["SHAPE@"] + [field[0] for field in stopFields]) as ICursor:
    with open(StopTable,'rb') as fStops:
        next(fStops)
        reader = csv.reader(fStops)
        
        # Put everything in utf-8 to handle BOMs and weird characters.
        # Eliminate blank rows (extra newlines) while we're at it.
        reader = ([x.decode('utf-8-sig').strip() for x in r] for r in reader if len(r) > 0)
        for stop in reader:
             # construct point geometry
             ptGeometry = arcpy.PointGeometry(arcpy.Point(float(stop[4]), float(stop[3])))
             stopGeomDict[int(stop[0])] = ptGeometry

             # insert our objects
             ICursor.insertRow((ptGeometry,stop[0],stop[1],stop[2],stop[5],stop[6]))
   
        

            
arcpy.AddMessage("Stops feature class created!\n\n")

arcpy.AddMessage("Generating transit line feature class.")

# (2)   Obtain schedule info from the stop_times.txt file
#       and convert it to a line-based model

stop_times_dict = {} # {trip_id: [stop_id, stop_sequence, arrival_time, departure_time]}
        # One entry per transit line connecting a unique pair of stops (with duplicate entries for different
        # route_type values connecting the same pair of stops). Size shouldn't be terribly much larger than the
        # number of stops for a normal network. Only central stations and transit hubs have large numbers of
        # connections.

routeDict = {}
tripsDict = {}

TripsTable = inGTFSdir + "\trips.txt"
with open(TripsTable, 'rb') as fTrips:
    next(fTrips)
    reader = csv.reader(fTrips)

    # Put everything in utf-8 to handle BOMs and weird characters.
    # Eliminate blank rows (extra newlines) while we're at it.
    reader = ([x.decode('utf-8-sig').strip() for x in r] for r in reader if len(r) > 0)
    for row in reader:
        routeDict[row[2]] = row[0]


outLineFC = arcpy.CreateFeatureclass_management(FD,"transit","POLYLINE",spatial_reference=sr)

# read-in stop_times.txt
StopTimesTable = inGTFSdir + "\stop_times.txt"
with open(StopTimesTable, 'rb') as fStopTimes:
    next(fStopTimes)
    reader = csv.reader(fStopTimes)

     # Put everything in utf-8 to handle BOMs and weird characters.
     # Eliminate blank rows (extra newlines) while we're at it.
    reader = ([x.decode('utf-8-sig').strip() for x in r] for r in reader if len(r) > 0)


    for row in reader:
        if int(row[0]) in tripsDict:
            tripsDict[int(row[0])].append((int(row[3]), int(row[4])))
        else:            
            tripsDict[int(row[0])] = [(int(row[3]), int(row[4]))]

##     # First row is column names:
##     columns = [name.strip() for name in reader.next()]
##     
##     # locate each field in each rows
##     idx_trip_id = columns.index("trip_id")
##     idx_stop_id = columns.index("stop_id")
##     idx_stop_sequence = columns.index("stop_sequence")
##     idx_arrival_time = columns.index("arrival_time")
##     idx_departure_time = columns.index("departure_time")

outPairStops = arcpy.CreateFeatureclass_management(FD,"pairStops","POINTS",spatial_reference=sr)
arcpy.management.AddField(outPairStops, "LINEID", "SHORT")
arcpy.management.AddField(outPairStops, "SEQ", "SHORT")

stopPairSet = []

with arcpy.da.InsertCursor(outPairStops, ["SHAPE@", "ROUTEID", "SEQ"]) as ICursor:
    for tripID in tripsDict.keys():
        arr = arcpy.Array()
        trip = sorted(tripsDict[tripID], key=itemgetter(1))
        for x in range(0, len(trip)-1):
            lineID = "%s , %s , %s" % (trip[x][0], trip[x+1][0], str(routeDict[trip]))
            if lineID not in stopPairSet:
                stopPairSet.append(lineID)
    for i in stopPairSet:
        data = i.split(',')
        ICursor.insertRow([stopGeomDict[int(data[0])], i, 1])
        ICursor.insertRow([stopGeomDict[int(data[1])], i, 1])
    
##        trip_id = row[idx_trip_id]
##          stop_id = row[idx_stop_id]
##          stop_sequence = int(row[idx_stop_sequence])
##          arrival_time = hms.str2sec(row[idx_arrival_time])
##          departure_time = hms.str2sec(row[idx_departure_time])
##          datarow = [stop_id,stop_sequence,arrival_time,departure_time]  
##          stop_times_dict.setdefault(trip_id,[]).append(datarow)
##
##      # for each trip, select stops in the trip, put them in order and get pairs
##      # of directly-connected stops
##     for trip in stop_times_dict.keys():
##         selectedstops = stop_times_dict[trip]
##         selectedstops.sort(key=operator.itemgetter(1))
##         for x in range(0,len(selectedstops)-1):
##             start_stop = selectedstops[x][0]
##             end_stop = selectedstops[x+1][0]
##             
##             SourceOIDkey = "%s, %s" % (start_stop, end_stop)
##
##             # SourceOIDkey = "%s, %s, %s" % (start_stop, end_stop, str(trip_routetype_dict[trip]))
##             # this stop paris needs a line feature
##             linefeature_dict[SourceOIDkey] = True
##
##
##      # ----- Write pairs to a points feature class
##      # (this is intermediate and will NOT go into the final ND) ----
##   
##      # create a points feature class for the point pairs
##outStopPairs = arcpy.management.CreateFeatureclass(outFD, "StopPairs", "POINT", "", "", "")
##arcpy.management.AddField(outStopPairs, "stop_id", "TEXT")
##arcpy.management.AddField(outStopPairs, "pair_id", "TEXT")
##arcpy.management.AddField(outStopPairs, "sequence", "SHORT")
##
##      # add pairs of stops to the feature calss in preparation for generating line features
##
##badStops, badkeys = [],[] # ??? this line seems redundant
##
##with arcpy.da.InsertCursor(outStopPairs, ["SHAPE@", "stop_id", "pair_id", "sequence"]) as cursor2:
### linefeature_dict = {"start_stop , end_stop , route_type": True}
##     for SourceOIDkey in linefeature_dict:
##          stopPair = SourceOIDkey.split(" , ")
##          # {stop_id: [stop_lat, stop_lon]}
##          stop1 = stopPair[0]
##          stop1_geom = stoplatlon_dict[stop1]  ##### CHECK THIS BUG NEXT TIME!
##          stop2 = stopPair[1]
##          stop2_geom = stoplatlon_dict[stop2]
##                
##          cursor2.insertRow((stop1_geom, stop1, SourceOIDkey, 1))
##          cursor2.insertRow((stop2_geom, stop2, SourceOIDkey, 2))
##                
##                
##      # ----- Generate lines between all stops (for the final ND) -----
##
### defining workspace here
##arcpy.env.workspace = outFD
##
##
##outLines = arcpy.management.PointsToLine(outStopPairs, "Routes","pair_id", "sequence")
##arcpy.management.AddField(ourLines, "route_type", "SHORT")
##     
##arcpy.management.AddField(outLines, "route_type_text", "TEXT") # ??? is this redundant?
##
### We don't need the points for anything anymore, so delete them.
##arcpy.Delete_management(outStopPairs)
##
### Clean up lines with 0 length.  They will just produce build errors and
### are not valuable for the network dataset in any other way.
##expression = """"Shape_Length" = 0"""
##with arcpy.da.UpdateCursor(outLines, ["pair_id"], expression) as cursor3:
##     for row in cursor3:
##          del linefeature_dict[row[0]]
##          cursor3.deleteRow()
##
### insert the route type to the output lines
##       
##            
##
##print "done!\n"
##
##"""
##try 1st (this prints out everything):
##StopTable = inGTFSdir + "\stops.txt"
##with open(StopTable,'rb') as f:
##    reader = csv.reader(f)
##    for row in reader:
##        print row
##
##"""
##
##"""
##try 2nd:
##
##arcpy.management.AddField(outStopsFC, "stop_id", "TEXT")
##arcpy.management.AddField(outStopsFC, "stop_name", "TEXT")
##arcpy.management.AddField(outStopsFC, "stop_desc", "TEXT")
##arcpy.management.AddField(outStopsFC, "stop_lat", "FLOAT")
##arcpy.management.AddField(outStopsFC, "stop_lon", "FLOAT")
##arcpy.management.AddField(outStopsFC, "zone_id", "TEXT")
##arcpy.management.AddField(outStopsFC, "stop_url", "TEXT")
##arcpy.management.AddField(outStopsFC, "location_type", "TEXT")
##arcpy.management.AddField(outStopsFC, "parent_station", "TEXT")
##arcpy.management.AddField(outStopsFC, "stop_timezone", "TEXT")
##arcpy.management.AddField(outStopsFC, "wheelchair_boarding", "TEXT")
##
##
##with arcpy.da.InsertCursor(outStopsFC, ["SHAPE@", "stop_id",
##                                                 "stop_code", "stop_name", "stop_desc",
##                                                 "zone_id", "stop_url", "location_type",
##                                                 "parent_station", "wheelchair_boarding"]) as cur1:
##    for stop in StopTable:
##        stop_id = stop[0]
##        
##
##
##
##
##"""
##
##"""
##try 3rd:
##rownum = 0
##for row in reader:
##    if rownum == 0:
##        header = row
##        for field in header:
##            #print type(field)
##    rownum +=1
##    
##print "\nheader row is:",header
##"""
##
##
##
##
