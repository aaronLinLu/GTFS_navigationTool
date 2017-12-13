# Name: CreateFeaturedataset
# Author: Lin 
# Description: Create a feature dataset 

# Import system modules
import arcpy
print "good"
from arcpy import env
import sqlize_csv
import hms

# enable file overwrite
arcpy.env.overwriteOutput = True

# Set workspace
env.workspace = "E:\Geog 173 workspace\Final Project\Lins Workspace\Workspace"

# Set local variables
out_dataset_path = "E:\Geog 173 workspace\Final Project\Lins Workspace\Network Dataset\GTFSAnalysis.gdb" 
out_name = "AnalysisResults"

# Creating a spatial reference object
sr = arcpy.SpatialReference(4326)
print sr

# Create a FileGDB for the feature dataset
arcpy.CreateFileGDB_management("E:\Geog 173 workspace\Final Project\Lins Workspace\Network Dataset", "GTFSAnalysis.gdb")

# Execute CreateFeaturedataset 
arcpy.CreateFeatureDataset_management(out_dataset_path, out_name, sr)


# print "Done!!!!"
