#-------------------------------------------------------------------------------
# Name:        module1
# Purpose:
#
# Author:      cchen
#
# Created:     10/02/2017
# Copyright:   (c) cchen 2017
# Licence:     <your licence>
#-------------------------------------------------------------------------------
import sys, os, string, arcpy, logging
from arcpy import env, mapping
import json as simplejson
import urllib,math

## if Elevation field does not exist, create one and insert value. project the shapefile to have the same coordinate system as the inhouse DEM (GCS 83)
try:

# scratch#
    scratch = arcpy.env.scratchWorkspace
    #scratch = r"E:\GISData_testing\temp\elevation_tool_temp\inhouse"

#####static##########################
    imgdir_dem = r"\\Cabcvan1fpr009\US_DEM\DEM13"
    imgdir_demCA = r"\\Cabcvan1fpr009\US_DEM\DEM1"
    masterlyr_dem = r"\\Cabcvan1fpr009\US_DEM\CellGrid_1X1Degree_NW_imagename_update.shp"
    masterlyr_demCA = r"\\Cabcvan1fpr009\US_DEM\Canada_DEM_edited.shp"

    arcpy.env.outputCoordinateSystem = arcpy.Describe(masterlyr_dem).spatialReference
    arcpy.env.OverWriteOutput = True

    all_merge = arcpy.GetParameter(0)
    all_merge = str(all_merge)
    check_field = arcpy.ListFields(all_merge,"Elevation")
    if len(check_field)==0:
        arcpy.AddField_management(all_merge, "Elevation", "DOUBLE", "12", "6", "", "", "NULLABLE", "NON_REQUIRED", "")

    with arcpy.da.UpdateCursor(all_merge,["Elevation"]) as uc:
            for row in uc:
                row[0]=-999
                uc.updateRow(row)
            else:
                arcpy.SetParameter(1,all_merge)
    del uc

    imgs = []

    masterLayer_dem = arcpy.mapping.Layer(masterlyr_dem)
    arcpy.SelectLayerByLocation_management(masterLayer_dem, 'intersect', all_merge)
    if int((arcpy.GetCount_management(masterLayer_dem).getOutput(0))) != 0:
        columns = arcpy.SearchCursor(masterLayer_dem)
        for column in columns:
            img = column.getValue("image_name")
            if img.strip() !="":
                imgs.append(os.path.join(imgdir_dem,img))
                print "found img " + img
        del column
        del columns

    masterLayer_dem = arcpy.mapping.Layer(masterlyr_demCA)
    arcpy.SelectLayerByLocation_management(masterLayer_dem, 'intersect', all_merge)
    if int((arcpy.GetCount_management(masterLayer_dem).getOutput(0))) != 0:

        columns = arcpy.SearchCursor(masterLayer_dem)
        for column in columns:
            img = column.getValue("image_name")
            if img.strip() !="":
                imgs.append(os.path.join(imgdir_demCA,img))
                print "found img " + img
        del column
        del columns


    if imgs != []:
        all_merge_1 = all_merge[:-4]+"_inhouse.shp"
        arcpy.Project_management(all_merge,all_merge_1,arcpy.Describe(masterlyr_dem).spatialReference)
        all_merge = all_merge_1
        del all_merge_1

        eleList=[]
        a =arcpy.Describe(all_merge)
        for img in imgs:
            adem =arcpy.Describe(img)
            mch = arcpy.Describe(img).meanCellHeight
            mcw = arcpy.Describe(img).meanCellWidth

            if a.Extent.lowerLeft.X>=adem.Extent.lowerLeft.X:
                ulx = adem.Extent.lowerLeft.X+ int((a.Extent.lowerLeft.X-adem.Extent.lowerLeft.X)/mcw)*mcw
                uly = adem.Extent.lowerLeft.Y+ int((a.Extent.lowerLeft.Y-adem.Extent.lowerLeft.Y)/mch)*mch
            else:
                ulx = adem.Extent.lowerLeft.X- math.ceil((adem.Extent.lowerLeft.X-a.Extent.lowerLeft.X)/mcw)*mcw
                uly = adem.Extent.lowerLeft.Y- math.ceil((adem.Extent.lowerLeft.Y-a.Extent.lowerLeft.Y)/mch)*mch
            ele = arcpy.RasterToNumPyArray(img,a.Extent.lowerLeft,math.ceil((a.Extent.XMax-ulx)/mch),math.ceil((a.Extent.YMax-uly)/mcw))
            if len(ele)==1:
                with arcpy.da.UpdateCursor(all_merge,["SHAPE@XY","Elevation"]) as uc:
                    for row in uc:
                        if row[1]==-999:
                            if ele[0,0] >-50:
                                row[1]= ele[0,0]
                                uc.updateRow(row)
                del ele
                del row
                del uc
            elif len(ele)!=0:
                with arcpy.da.UpdateCursor(all_merge,["SHAPE@XY","Elevation"]) as uc:
                    for row in uc:
                        if row[1]==-999:
                            x,y = row[0]
                            deltaX = x - ulx
                            deltaY = uly- y
                            arow =math.floor(deltaY/mch)
                            acol = math.floor(deltaX/mcw)

                            if ele[int(arow),int(acol)] >-50:
                                row[1]= ele[int(arow),int(acol)]
                                uc.updateRow(row)

                        eleList.append(row[1])
                del ele
                del row
                del uc

    else:
        rows = arcpy.da.UpdateCursor(all_merge,["ID","SHAPE@XY","Elevation"])
        for row in rows:
            row[2] = -999
            rows.updateRow(row)
        #del row
        del rows

    arcpy.SetParameter(1,all_merge)


except:
   # If an error occurred, print the message to the screen
   arcpy.AddMessage(arcpy.GetMessages())
   raise





