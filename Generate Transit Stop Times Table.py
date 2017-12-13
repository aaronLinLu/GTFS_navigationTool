# Name: Generate Transit Stop Times Table
# Author: wyeung
# Description: Create a attribute table for all the stop times

#import modules
import arcpy, sqlize_csv, hms, sqlite3, os, operator, itertools, csv, re
from arcpy import env

#allow overwrite
arcpy.env.overwriteOutput = True

#GTFS directories
workspace = r'F:\Final Project\Lins Workspace\Workspace'
inGTFSdir = r'F:\Final Project\Lins Workspace\raw GTFS Google\google_transit_20160328_v1'
outFD = r'F:\Final Project\Lins Workspace\Network Dataset\GTFSAnalysis.gdb'

#delete existing table
fc = 'stop_times.dbf'
arcpy.Delete_management(fc)


#create dbase table
outtable = arcpy.CreateTable_management(outFD, fc)

#read stop_times.txt
stopTime_table = inGTFSdir + '\stop_times.txt'
f = open(stopTime_table, 'rb')
reader = csv.reader(f)

#add fields in table
arcpy.AddField_management(outtable, 'trip_id', "TEXT")

arcpy.AddField_management(outtable, 'arrival_t', "FLOAT")
arcpy.AddField_management(outtable, 'depart_t', "FLOAT")

arcpy.AddField_management(outtable, 'stop_id', "TEXT")
arcpy.AddField_management(outtable, 'stop_seq', "SHORT")
arcpy.AddField_management(outtable, 's_headsign', "TEXT")
arcpy.AddField_management(outtable, 'pickup_t', "TEXT")
arcpy.AddField_management(outtable, 'dropoff_t', "TEXT")
arcpy.AddField_management(outtable, 'shapetrav', "TEXT")
arcpy.AddField_management(outtable, 'timepoint', "SHORT")

arcpy.AddField_management(outtable,'arr_t_str',"TEXT")
arcpy.AddField_management(outtable,'dep_t_str',"TEXT")

#delete unwanted field
arcpy.DeleteField_management(outtable, 'Field1')

#loop through all the entries in txt
with arcpy.da.InsertCursor(outtable, ['trip_id', 'arrival_t', 'depart_t', 'stop_id',
                                      'stop_seq', 's_headsign', 'pickup_t', 'dropoff_t',
                                      'shapetrav', 'timepoint','arr_t_str','dep_t_str']) as ICursor:
    with open(stopTime_table, 'rb') as f:
        next(f)
        reader = csv.reader(f)
        for row in reader:
            trip_id = row[0]
            
            arrival_t = hms.str2sec(row[1])
            depart_t = hms.str2sec(row[2])
            stop_id = row[3]
            stop_seq = row[4]
            s_headsign = row[5]
            pickup_t = row[6]
            dropoff_t = row[7]
            shapetrav = row[8]
            timepoint = row[9]
            arr_t_str = row[1]
            dep_t_str = row[2]
            ICursor.insertRow((trip_id, arrival_t, depart_t, stop_id, stop_seq, s_headsign, pickup_t, dropoff_t, shapetrav, timepoint, arr_t_str, dep_t_str))
          


print 'done'
