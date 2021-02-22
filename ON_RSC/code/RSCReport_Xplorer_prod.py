### -*- coding: cp1252 -*-
 ########################################################
 ## Name: Create ERIS Ontario Maps
 ## Source Name: ERISReport.py
 ## Version: ArcGIS 10
 ## Author: Chris North (43 North GIS)
 ## Usage: ERISLayout <MapType>, <OrderID>, <MidX>, <MidY>, <Scale>, <Buffer1Distance>, <Buffer2Distance>
 ## Required Arguments:
 ##    Map Type: String e.g. "OBM"
 ##         choice "OBM"
 ##         choice "Soils"
 ##         choice "Physiography"
 ##         choice "ANSI"
 ##         choice "Surficial Geology"
 ##         choice "Bedrock Geology"
 ##    Order ID Number: String e.g. "12345678901"
 ##    Mid X Longitude: Double e.g. -79.08
 ##    Mid Y Longitude: Double e.g. 44.2
 ##    Map Scale: Double e.g. 20000
 ##    Buffer 1 Distance: String e.g. "250 Meters"
 ##    Buffer 2 Distance: String e.g. "2000 Meters"
 ## Optional Arguments: None
 ## Output: PDF (Type File)
 ## Description: Generates the PDF file for the ERIS Ontario map series.
 ## Date December 2011
 ## Modified by Jian Liu, April 2014
 ## Modified by Jian Liu, April 2015
 ########################################################

# Import required modules
from arcpy import env, mapping
from collections import OrderedDict
import zipfile, shutil,cx_Oracle,urllib
import sys, os, string, arcpy
import re
import json

def zipdir(path, zip):
    for root, dirs, files in os.walk(path):
        for file in files:
            print file + " " + root
            arcname = os.path.relpath(os.path.join(root, file), os.path.join(path, '..'))
            zip.write(os.path.join(root, file), arcname)

def soilFeilds(listID):
        multi1 = r"\\cabcvan1gis006\GISData\RSC_Ontario\PDFToolboxes\OntarioReports.gdb\soil_multi2"
        multi2 = r"\\cabcvan1gis006\GISData\RSC_Ontario\PDFToolboxes\OntarioReports.gdb\soil_multi4"
        multi3 = r"\\cabcvan1gis006\GISData\RSC_Ontario\PDFToolboxes\OntarioReports.gdb\soil_multi5"
        columes1 = arcpy.SearchCursor(multi1)
        columes2 = arcpy.SearchCursor(multi2)
        columes3 = arcpy.SearchCursor(multi3)
        index1 = OrderedDict([('FLEX4', 'Component No'),
         ('FLEX5', 'Components(%)'),('FLEX3', 'Soil Name ID'),
         ('FLEX13', 'Surface Stoniness Class'),
         ('FLEX6', 'Slop Steepness(%)'),
         ('FLEX7', 'Slop Length(m)'),
         ('FLEX18','Drainage'),
         ('FLEX19','Hydrological Soil Groups'),
         ('FLEX20','Soil Texture of A Horizon'),
         ('FLEX14','Field Crops Capability'),
         ('FLEX15','First CLI Limitation Subclass'),
         ('FLEX16','Second CLI Limitation Subclass')])
        index2 =  OrderedDict([('FLEX6', 'Soil Name'),
         ('FLEX29', 'Water Table Charateristics'),
         ('FLEX28', 'Soil Drainage Class'),
         ('FLEX27', 'Kind of Surface Material'),
         ('FLEX43', 'Layer that Restricts Root Growth'),
         ('FLEX42', 'Type of Root Restricting Layer'),
         ('FLEX33', 'Parent Material 1|2|3'),
         ('FLEX41', 'Mode of Deposition 1|2|3'),
         ('FLEX37', 'Parent Material Chemical Property 1|2|3')])
        index3 =OrderedDict([('FLEX36', 'Depth(cm)'),
         ('FLEX37', 'Horizon'),
         ('FLEX6', 'Layer No'),
         ('FLEX15', 'Very Fine Sand(%)'),
         ('FLEX16', 'Total Sand(%)'),
         ('FLEX17', 'Total Silt(%)'),
         ('FLEX18', 'Total Clay(%)'),
         ('FLEX19', 'Organic Carbon(%)'),
         ('FLEX20', 'pH in Calc Chloride'),
         ('FLEX24', 'Saturated Hydraulic Conductivity(cm/h)'),
         ('FLEX30', 'Electrical Conductivity(dS/m)')])

        out1 ={}
        list1 = []
        keys = []
        pkey1to2 ={}

        for col in columes1:
            if col.OWNER_OID in listID:
                if col.OWNER_OID not in out1.keys():
                    for key in index1.keys():
                        list1.append(index1[key]+" : "+ str(col.getValue(key)))
                    keys.append(col.getValue("FLEXID"))
                    out1[col.OWNER_OID] = [list1]
                else:
                    keys = pkey1to2[col.OWNER_OID]
                    keys.append(col.getValue("FLEXID"))
                    for key in index1.keys():
                        list1.append(index1[key]+" : "+ str(col.getValue(key)))
                    out1[col.OWNER_OID].append(list1)
                pkey1to2[col.OWNER_OID] = keys
                list1=[]
                keys=[]
        del col
        del columes1

        out2 = {}
        list2 = []
        pkey2to3 = {}
        values1to2 = []

        for value in pkey1to2.values():
            values1to2.extend(value)

        for col2 in columes2:
            if col2.OWNER_OID in values1to2:
                if col2.OWNER_OID not in out2.keys():
                    for key in index2.keys():
                        list2.append(index2[key]+" : "+ str(col2.getValue(key)))
                    key2 = col2.getValue("FLEX45")
                    out2[col2.OWNER_OID] = [list2]
                else:
                    list2 = out1[col2.OWNER_OID]
                    key2 = col2.getValue("FLEX45")

                    for key in index2.keys():
                        list2.append(index2[key]+" : "+ str(col2.getValue(key)))
                    out2[col2.OWNER_OID] = [list2]

                pkey2to3[col2.OWNER_OID] = key2
                list2=[]
                keys=[]
        del col2
        del columes2

        out3 = {}
        list3 = []
        pkey3 = {}
        values2to3 = []
        values2to3 = pkey2to3.values()

        for col3 in columes3:
            if col3.OWNER_OID in values2to3:
                if col3.OWNER_OID not in out3.keys():
                    for key in index3.keys():
                        list3.append(index3[key]+" : "+ str(col3.getValue(key)))
                    out3[col3.OWNER_OID] = [list3]

                else:
                    list3 = out3[col3.OWNER_OID]
                    for key in index3.keys():
                        list3.append(index3[key]+" : "+ str(col3.getValue(key)))
                    out3[col3.OWNER_OID] = [list3]

                list3=[]
                keys=[]
        del col3
        del columes3

        for key in out2.keys():
             if key in out3.keys():
                 list2 = out2[key]
                 list2.append(out3[key])
                 out2[key] = list2

        list1 = []
        for key in pkey1to2.keys():
            values = pkey1to2[key]
            for value in values:
                if value in out2.keys():
                    value2 = out2[value]
                    list1.append(value2)
            pkey1to2[key] = list1
            list1 =[]


        for key in out1.keys():
             if key in pkey1to2.keys():
                 list2 = out1[key]
                 list22 = pkey1to2[key]
                 if len(list2) == len(list22):
                    for i in range(len(list2)):
                        list2[i].append(list22[i])
                 out1[key] = list2
        return out1

# By Setting OverWriteOutput to "True" the script will predictably, overwrite
# any files with the same name in the same directory
arcpy.env.overwriteOutput = True
arcpy.env.OverWriteOutput = True
# The try statement is used to handle errors. If an
# error is found within the try indentation you script will jump to the Except
# statement and run what ever code this there before ending.

# -------------------------------------------------------------------------------------------------------------------------------
# Set local variables.
# -------------------------------------------------------------------------------------------------------------------------------
outPointFileName = "point2.shp"
outBuffer2FileName = "Buffer2.shp"
outPolyFileName = "ReportPolygons.shp"

srGCS83 = arcpy.SpatialReference(r"\\cabcvan1gis006\GISData\RSC_Ontario\PDFToolboxes\PRJ\GCSNorthAmerican1983.prj")
srCanadaAlbers = arcpy.SpatialReference(r"\\cabcvan1gis006\GISData\RSC_Ontario\PDFToolboxes\PRJ\CanadaAlbersEqualAreaConic.prj")

# MapTypes = arcpy.GetParameterAsText(0)
# OrderNumText = arcpy.GetParameterAsText(1)
# AddressString = arcpy.GetParameterAsText(2)
# OrderType = arcpy.GetParameterAsText(3)
# OrderCoords =  eval(arcpy.GetParameterAsText(4))
# mapScale = float(arcpy.GetParameterAsText(5))
# buffer2DistanceValue = arcpy.GetParameterAsText(6)
# buffer2Distance = buffer2DistanceValue + " METERS"
# scratchfolder = arcpy.env.scratchWorkspace
# scratch = arcpy.env.scratchGDB

# Local -------------------------------------------------------------------------------------------------------------------------
MapTypes = 'OBM,Soils,Physiography,ANSI,Surficial Geology,Bedrock Geology'#OBM #Soils #Physiography #ANSI #Surficial Geology #Bedrock Geology
AddressString = ''#arcpy.GetParameterAsText(2)
OrderType = ''#arcpy.GetParameterAsText(3)
OrderCoords = []
mapScale ='22000' #float(arcpy.GetParameterAsText(5))
buffer2DistanceValue = '2000'#arcpy.GetParameterAsText(6)
buffer2Distance = buffer2DistanceValue + " METERS"

OrderNumText = '20311300062' #arcpy.GetParameterAsText(1)
scratchfolder = os.path.join(r'\\cabcvan1gis005\MISC_DataManagement\_AW\RSC_ON_SCRATCHY', OrderNumText)
if not os.path.exists(scratchfolder):
    os.mkdir(scratchfolder)
scratch =os.path.join(scratchfolder,"scratch.gdb")
arcpy.CreateFileGDB_management(scratchfolder,"scratch.gdb")

#################################################################################################
connectionString = r"eris_gis/gis295@cabcvan1ora003.glaciermedia.inc:1521/GMPRODC"
report_path = r"\\cabcvan1eap006\ErisData\Reports\prod\noninstant_reports"
reportcheck_path = r'\\cabcvan1eap006\ErisData\Reports\prod\reportcheck'
viewer_path = r"\\CABCVAN1eap006\ErisData\Reports\prod\viewer"
upload_link = r"http://CABCVAN1eap006/ErisInt/BIPublisherPortal_prod/Viewer.svc/"
#################################################################################################
try:
    con = cx_Oracle.connect(connectionString)
    cur = con.cursor()

    cur.execute("select order_id, address1, order_json from orders where order_num ='" + str(OrderNumText)+"'")
    t = cur.fetchone()
    OrderIDText = str(t[0])
    AddressString = str(t[1])
    # OrderType = re.search('("geometry_type":")(\w+)(",)', str(t[2])).group(2).strip()                 # does not get updated if changes were made
    # OrderCoords = json.loads(re.search('("geometry":")(\[\[\[.+\]\]\])(")', str(t[2])).group(2))      # does not get updated if changes were made

    cur.execute("select geometry_type,geometry from eris_order_geometry where order_id =" + OrderIDText)
    t = cur.fetchone()
    OrderType = str(t[0])
    OrderCoords =  json.loads(str(t[1]))
finally:
    cur.close()
    con.close()

point = arcpy.Point()
array = arcpy.Array()
sr = arcpy.SpatialReference()
sr.factoryCode = 4269
sr.XYTolerance = .00000001
sr.scaleFactor = 2000
sr.create()
featureList = []

for feature in OrderCoords:
    # For each coordinate pair, set the x,y properties and add to the
    # Array object.
    for coordPair in feature:
        point.X = coordPair[0]
        point.Y = coordPair[1]
        sr.setDomain (point.X, point.X, point.Y, point.Y)
        array.add(point)

    if OrderType.lower()== 'point':
	    feaERIS = arcpy.Multipoint(array, sr)
    elif OrderType.lower() =='polyline':
	    feaERIS  = arcpy.Polyline(array, sr)
    else :
	    feaERIS = arcpy.Polygon(array,sr)
    # Clear the array for future use
    array.removeAll()

    # Append to the list of Polygon objects
    featureList.append(feaERIS)

# Create a copy of the Polygon objects, by using featureList as input to
# the CopyFeatures tool.
outshp= os.path.join(scratchfolder,"orderGeoName.shp")
arcpy.CopyFeatures_management(featureList, outshp)
arcpy.DefineProjection_management(outshp, srGCS83)

# 2. Calculate Centroid of Geometry
del point
del array

arcpy.AddField_management(outshp, "xCentroid", "DOUBLE", 18, 11)
arcpy.AddField_management(outshp, "yCentroid", "DOUBLE", 18, 11)

xExpression = "!SHAPE.CENTROID.X!"
yExpression = "!SHAPE.CENTROID.Y!"

arcpy.CalculateField_management(outshp, "xCentroid", xExpression, "PYTHON_9.3")
arcpy.CalculateField_management(outshp, "yCentroid", yExpression, "PYTHON_9.3")

# 3. Create a shapefile of centroid
in_rows = arcpy.SearchCursor(outshp)
outPointSHP = os.path.join(scratchfolder, outPointFileName)
point1 = arcpy.Point()
array1 = arcpy.Array()

featureList = []
arcpy.CreateFeatureclass_management(scratchfolder, outPointFileName, "POINT", "", "DISABLED", "DISABLED", srGCS83)
cursor = arcpy.InsertCursor(outPointSHP)
feat = cursor.newRow()

for in_row in in_rows:
    # Set X and Y for start and end points
    point1.X = in_row.xCentroid
    point1.Y = in_row.yCentroid
    array1.add(point1)
    # Create a Polyline object based on the array of points
    centerpoint = arcpy.Multipoint(array1)
    # Clear the array for future use
    array1.removeAll()
    # Append to the list of Polyline objects
    featureList.append(centerpoint)
    # Insert the feature
    feat.shape = point1
    cursor.insertRow(feat)

del feat
del in_rows
del cursor
del point1
del array1

arcpy.AddXY_management(outPointSHP)
outBuffer2SHP = os.path.join(scratchfolder, outBuffer2FileName)
arcpy.Buffer_analysis(outPointSHP, outBuffer2SHP, buffer2Distance)
# arcpy.Buffer_analysis(outshp, outBuffer2SHP, buffer2Distance)
MapTypesArray = MapTypes.strip(",").split(",")

for MapType in MapTypesArray:
    # -------------------------------------------------------------------------------------------------------
    # Part I: Create the Map Layout Page
    # -------------------------------------------------------------------------------------------------------
    # get the map document
    # Based on the choice of map, choose the correct MXD...
    # Choices are "OBM", "Soils", "Physiography", "ANSI", "Surficial Geology" or "Bedrock Geology"
    if MapType.upper() == "OBM".upper():
        mxd = arcpy.mapping.MapDocument(r"\\cabcvan1gis006\GISData\RSC_Ontario\PDFToolboxes\MXD\ERISOBMLayout.mxd")
    elif MapType.upper() == "Soils".upper():
        mxd = arcpy.mapping.MapDocument(r"\\cabcvan1gis006\GISData\RSC_Ontario\PDFToolboxes\MXD\ERISSoilsLayout_new.mxd")
    elif MapType.upper() == "Physiography".upper():
        mxd = arcpy.mapping.MapDocument(r"\\cabcvan1gis006\GISData\RSC_Ontario\PDFToolboxes\MXD\ERISPhysiographyLayout.mxd")
    elif MapType.upper() == "ANSI".upper():
        mxd = arcpy.mapping.MapDocument(r"\\cabcvan1gis006\GISData\RSC_Ontario\PDFToolboxes\MXD\ERISANSILayout.mxd")
    elif MapType.upper() == "Surficial Geology".upper():
        mxd = arcpy.mapping.MapDocument(r"\\cabcvan1gis006\GISData\RSC_Ontario\PDFToolboxes\MXD\ERISSurfaceGeologyLayout.mxd")
    elif MapType.upper() == "Bedrock Geology".upper():
        mxd = arcpy.mapping.MapDocument(r"\\cabcvan1gis006\GISData\RSC_Ontario\PDFToolboxes\MXD\ERISBedrockGeologyLayout.mxd")
    else:
        print("Error in map type!!")

    # Get the data frame
    df = arcpy.mapping.ListDataFrames(mxd,"*")[0]
    newLayerBuffer2 = arcpy.mapping.Layer(os.path.join(r"\\cabcvan1gis006\GISData\RSC_Ontario\PDFToolboxes\LYR",r"Buffer2.lyr"))
    newLayerBuffer2.replaceDataSource(scratchfolder, "SHAPEFILE_WORKSPACE", "Buffer2")
    arcpy.mapping.AddLayer(df, newLayerBuffer2, "TOP")

    if OrderType.lower()== 'point':
        geom= os.path.join(r"\\cabcvan1gis006\GISData\RSC_Ontario\PDFToolboxes\LYR",r"orderPoint.lyr")
    elif OrderType.lower() =='polyline':
        geom= os.path.join(r"\\cabcvan1gis006\GISData\RSC_Ontario\PDFToolboxes\LYR",r"orderLine.lyr")
    else:
        geom = os.path.join(r"\\cabcvan1gis006\GISData\RSC_Ontario\PDFToolboxes\LYR",r"orderPoly.lyr")

    newLayerordergeo = arcpy.mapping.Layer(geom)
    newLayerordergeo.replaceDataSource(scratchfolder, "SHAPEFILE_WORKSPACE", "orderGeoName" )
    arcpy.mapping.AddLayer(df, newLayerordergeo , "TOP")
    arcpy.RefreshActiveView()
    arcpy.RefreshTOC()

    OrderNumTextElement = arcpy.mapping.ListLayoutElements(mxd, "TEXT_ELEMENT", "OrderIDText")[0]
    # Get reference to the Sample Buffer Distance Text element
    if (MapType.upper() != "OBM" and MapType.upper() != "PHYSIOGRAPHY"):
        SampleBufferDistanceTextElement = arcpy.mapping.ListLayoutElements(mxd, "TEXT_ELEMENT", "SampleBufferDistanceText")[0]

    df.panToExtent(newLayerBuffer2.getExtent())
    df.scale = mapScale
    arcpy.RefreshActiveView()
    arcpy.RefreshTOC()

    # Set the spatial reference...
    df.spatialReference = srCanadaAlbers
    # Change the Order ID on the map to reflect the current Order...
    OrderNumTextElement.text = "Order No. " + OrderNumText + ""
    # Change the Buffer distance on the map sample area to reflect the buffere distance.
    if (MapType.upper() == "OBM" or MapType.upper() == "PHYSIOGRAPHY"):
        for lyr in arcpy.mapping.ListLayers(mxd, "", df):
            if lyr.name == "Buffer2":
                arcpy.mapping.RemoveLayer(df, lyr)
    else:
        SampleBufferDistanceTextElement.text = buffer2DistanceValue + "m Buffer"
    
    noSoil = False

    if MapType == "Soils":
        outPolySHP1 = os.path.join(scratch, "soil_new")
        outBuffer2SHP1 = os.path.join(scratchfolder, "buffer_soil.shp")
        arcpy.Buffer_analysis(outPointSHP, outBuffer2SHP1, str(float(buffer2DistanceValue)*2) + " METERS")
        masterLayer_soil = arcpy.mapping.Layer(r"\\cabcvan1gis006\GISData\RSC_Ontario\PDFToolboxes\OntarioReports.gdb\master_soil")
        arcpy.SelectLayerByLocation_management(masterLayer_soil,'intersect',outBuffer2SHP)

        if (int((arcpy.GetCount_management(masterLayer_soil).getOutput(0))) !=0):
            arcpy.Clip_analysis(r"\\cabcvan1gis006\GISData\RSC_Ontario\PDFToolboxes\OntarioReports.gdb\ON_v3", outBuffer2SHP1, outPolySHP1,"0.000001 DecimalDegrees")
            newLayerSoil = arcpy.mapping.Layer(os.path.join(r"\\cabcvan1gis006\GISData\RSC_Ontario\PDFToolboxes\LYR",r"soil_new.lyr"))
            newLayerSoil.replaceDataSource(scratch, "FILEGDB_WORKSPACE", "soil_new" )

            if newLayerSoil.symbologyType == "UNIQUE_VALUES":
                newLayerSoil.symbology.valueField = "POLY_ID"
                newLayerSoil.symbology.addAllValues()
            arcpy.SaveToLayerFile_management(newLayerSoil,  os.path.join(scratchfolder,"soil_new.lyr"), "ABSOLUTE")
            arcpy.mapping.AddLayer(df, newLayerSoil, "BOTTOM")
        else:
            noSoil = True
            pageCounter = 1

    outputLayoutPDF = os.path.join(scratchfolder, 'mapExport.pdf')
    arcpy.mapping.ExportToPDF(mxd, outputLayoutPDF, "PAGE_LAYOUT", 640, 480, 300, "BEST", "RGB", True, "NONE", "RASTERIZE_BITMAP", False, True, "LAYERS_AND_ATTRIBUTES", True, 80)

    if MapType.upper() == "OBM".upper():
        mxd.saveACopy(os.path.join(scratchfolder,"ERISOBMLayout.mxd"))
    elif MapType.upper() == "Soils".upper():
        mxd.saveACopy(os.path.join(scratchfolder,"ERISSoilsLayout.mxd"))
    elif MapType.upper() == "Physiography".upper():
        mxd.saveACopy(os.path.join(scratchfolder,"ERISPhysiographyLayout.mxd"))
    elif MapType.upper() == "ANSI".upper():
        mxd.saveACopy(os.path.join(scratchfolder,"ERISANSILayout.mxd"))
    elif MapType.upper() == "Surficial Geology".upper():
        mxd.saveACopy(os.path.join(scratchfolder,"ERISSurfaceGeologyLayout.mxd"))
    elif MapType.upper() == "Bedrock Geology".upper():
        mxd.saveACopy(os.path.join(scratchfolder,"ERISBedrockGeologyLayout.mxd"))

    if MapType =='OBM':
        obmlist=[]
        masterLayer_OBM = arcpy.mapping.Layer(r"\\cabcvan1gis006\GISData\RSC_Ontario\PDFToolboxes\masterfile\OBM_masterfile.shp")
        arcpy.SelectLayerByLocation_management(masterLayer_OBM,'intersect',outPointSHP)

        if (int((arcpy.GetCount_management(masterLayer_OBM).getOutput(0))) !=0):
            deliverfolder = os.path.join(scratchfolder, OrderNumText+'_RSC_Order')

            if not os.path.exists(deliverfolder):
                os.makedirs(deliverfolder)

            rows = arcpy.SearchCursor(masterLayer_OBM)

            for row in rows:
                shutil.copy(os.path.join(r"\\Cabcvan1fpr009\data_gis_2\OBM",str(row.Sheet)+".tif"),os.path.join(deliverfolder,str(row.Sheet)+".tif"))
                obmlist.append(str(row.Sheet)) 

    # ---------------------------------------------------------------------------------------------
    # Part II: Create the Report Page(s)
    # ---------------------------------------------------------------------------------------------
    # get the map document
    # Based on the choice of map, choose the correct Report MXD...
    # Choices are "Soils",  "ANSI", "Surficial Geology" or "Bedrock Geology"
    # Note: There is no 'report' for an "OBM" or "Physiography" map, so they're not a choice here...
    needViewer = 'Y'
    if needViewer == 'Y':
        # clip wetland, flood, geology, soil and covnert .lyr to kml
        # for now, use clipFrame_topo to clip
        # added clip current topo
        viewerdir_kml = os.path.join(scratchfolder,OrderNumText+'_rsckml')

        if not os.path.exists(viewerdir_kml):
            os.mkdir(viewerdir_kml)
        viewerdir_topo = os.path.join(scratchfolder,OrderNumText+'_rscimage')

        if not os.path.exists(viewerdir_topo):
            os.mkdir(viewerdir_topo)
        viewertemp =os.path.join(scratchfolder,'viewertemp')

        if not os.path.exists(viewertemp):
            os.mkdir(viewertemp)

        ################## Xplorer ################################
        srGoogle = arcpy.SpatialReference(3857)   # web mercator
        srWGS84 = arcpy.SpatialReference(4326)    # WGS84

    if MapType =='OBM':
        ################## EXPLORER ###############################
        # current topo clipping for Xplorer
        imagename = r"topoclip.jpg"
        mxdname = os.path.join(scratchfolder,"ERISOBMLayout.mxd")
        mxd = arcpy.mapping.MapDocument(mxdname)
        df = arcpy.mapping.ListDataFrames(mxd,"*")[0]    # the spatial reference here is UTM zone #, need to change to WGS84 Web Mercator
        df.spatialReference = srWGS84

        for lyr in arcpy.mapping.ListLayers(mxd, "", df):
            if lyr.name == "SiteMaker" or lyr.name =='Project Property':
                arcpy.mapping.RemoveLayer(df, lyr)

        arcpy.mapping.ExportToJPEG(mxd, os.path.join(viewerdir_topo,imagename), df, df_export_width=1950,df_export_height=2000, world_file = True, resolution = 100)
        arcpy.DefineProjection_management(os.path.join(viewerdir_topo,imagename), srWGS84)
        extent = arcpy.Describe(os.path.join(viewerdir_topo,imagename)).Extent.projectAs(srWGS84)
        metaitem = {}
        metaitem['type'] = 'rscb'
        metaitem['imagename'] = imagename
        metaitem['lat_sw'] = extent.YMin
        metaitem['long_sw'] = extent.XMin
        metaitem['lat_ne'] = extent.YMax
        metaitem['long_ne'] = extent.XMax
        del df, mxd

        try:
            con = cx_Oracle.connect(connectionString)
            cur = con.cursor()

            cur.execute("delete from overlay_image_info where  order_id = %s and (type = 'rscb')" % str(OrderIDText))
            cur.execute("insert into overlay_image_info values (%s, %s, %s, %.5f, %.5f, %.5f, %.5f, %s, '', '')" % (str(OrderIDText), str(OrderNumText), "'" + metaitem['type']+"'", metaitem['lat_sw'], metaitem['long_sw'], metaitem['lat_ne'], metaitem['long_ne'],"'"+metaitem['imagename']+"'" ) )
            con.commit()
        finally:
            cur.close()
            con.close()

#        ##################EXPLORER###############################
#        # current topo clipping for Xplorer
#        if obmlist !=[]:
#            for i in range(len(obmlist)):
#                imagename = 'topoclip_%s.jpg'%i
#                shutil.copy(os.path.join(r"\\cabcvan1gis001\DATA_GIS_2\OBM",obmlist[i]+".jpg"),os.path.join(viewerdir_topo,imagename))
#                   shutil.copy(os.path.join(r"\\cabcvan1gis001\DATA_GIS_2\OBM",obmlist[i]+"_g.tif")),scratchfolder)
#                   georefereced_input = os.path.join(scratchfolder,obmlist[i]+"_g.tif")

#                   arcpy.DefineProjection_management(georefereced_input srWGS84)
#                arcpy.MakeFeatureLayer_management (r"E:\GISData\RSC_Ontario\PDFToolboxes\masterfile\OBM_masterfile1.shp", r"masterfile")
#                arcpy.SelectLayerByAttribute_management("masterfile",'NEW_SELECTION',"'Sheet=%s'"%obmlist[i])

#                extent = arcpy.Describe("masterfile").Extent.projectAs(srWGS84)
#                metaitem = {}
#                metaitem['type'] = 'rscb'
#                metaitem['imagename'] = imagename
#                metaitem['lat_sw'] = extent.YMin
#                metaitem['long_sw'] = extent.XMin
#                metaitem['lat_ne'] = extent.YMax
#                metaitem['long_ne'] = extent.XMax

#                try:
#                    con = cx_Oracle.connect(connectionString)
#                    cur = con.cursor()

#                    cur.execute("delete from overlay_image_info where  order_id = %s and (type = 'rscb')" % str(OrderIDText))
#                    cur.execute("insert into overlay_image_info values (%s, %s, %s, %.5f, %.5f, %.5f, %.5f, %s, '', '')" % (str(OrderIDText), str(OrderNumText), "'" + metaitem['type']+"'", metaitem['lat_sw'], metaitem['long_sw'], metaitem['lat_ne'], metaitem['long_ne'],"'"+metaitem['imagename']+"'" ) )
#                    con.commit()

#                finally:
#                    cur.close()
#                    con.close()

    elif MapType =='Physiography':
        # current topo clipping for Xplorer
        imagename = r"physclip.jpg"
        mxdname = os.path.join(scratchfolder,"ERISPhysiographyLayout.mxd")
        mxd = arcpy.mapping.MapDocument(mxdname)
        df = arcpy.mapping.ListDataFrames(mxd,"*")[0]    # the spatial reference here is UTM zone #, need to change to WGS84 Web Mercator
        df.spatialReference = srWGS84

        for lyr in arcpy.mapping.ListLayers(mxd, "", df):
            if lyr.name == "SiteMaker" or lyr.name =='Project Property':
                arcpy.mapping.RemoveLayer(df, lyr)

        arcpy.mapping.ExportToJPEG(mxd, os.path.join(viewerdir_topo,imagename), df, df_export_width=1950,df_export_height=2000, world_file = True, resolution = 100)
        arcpy.DefineProjection_management(os.path.join(viewerdir_topo,imagename), srWGS84)
        extent = arcpy.Describe(os.path.join(viewerdir_topo,imagename)).Extent.projectAs(srWGS84)
        metaitem = {}
        metaitem['type'] = 'rscp'
        metaitem['imagename'] = imagename
        metaitem['lat_sw'] = extent.YMin
        metaitem['long_sw'] = extent.XMin
        metaitem['lat_ne'] = extent.YMax
        metaitem['long_ne'] = extent.XMax
        del df, mxd

        try:
            con = cx_Oracle.connect(connectionString)
            cur = con.cursor()
            cur.execute("delete from overlay_image_info where  order_id = %s and (type = 'rscp')" % str(OrderIDText))
            cur.execute("insert into overlay_image_info values (%s, %s, %s, %.5f, %.5f, %.5f, %.5f, %s, '', '')" % (str(OrderIDText), str(OrderNumText), "'" + metaitem['type']+"'", metaitem['lat_sw'], metaitem['long_sw'], metaitem['lat_ne'], metaitem['long_ne'],"'"+metaitem['imagename']+"'" ) )
            con.commit()
        finally:
            cur.close()
            con.close()
# -------------------------------------------------------------------------------------------------------------------------------
    elif MapType == "Soils":
        reportmxd = arcpy.mapping.MapDocument(r"\\cabcvan1gis006\GISData\RSC_Ontario\PDFToolboxes\MXD\ERISSoilsReport_new.mxd")
        soilshp = os.path.join(scratch, "soilshp")

        if noSoil==False:
            arcpy.Clip_analysis(outPolySHP1, outBuffer2SHP, soilshp)
            soilPolykeys = []
            polygonRows = arcpy.SearchCursor(soilshp)

            for row in polygonRows:
                poly_id = row.POLY_ID
                soilPolykeys.append(poly_id)
            # Now, create a search cursor for the output polygons...
            fieldsSoil = soilFeilds(soilPolykeys)
            # fieldsSoil[u'OND002094882'] = out1[u'OND002094882']
            pageCounter = 1
            textBoxCounter = 1
            soilBox = 1
            count =0
            soilFieldsKML ={}

            for key in fieldsSoil.keys():
                count = count + len(fieldsSoil[key])

            while soilBox <= count:
                for key in fieldsSoil.keys():
                    for component in fieldsSoil[key]:
                        # There are ten (10) text boxes on the SOILS Report Page....
                        if textBoxCounter == 1:
                            currentTextBoxElement = arcpy.mapping.ListLayoutElements(reportmxd, "TEXT_ELEMENT", "TextBox1")[0]
                        if textBoxCounter == 2:
                            currentTextBoxElement = arcpy.mapping.ListLayoutElements(reportmxd, "TEXT_ELEMENT", "TextBox2")[0]
                        if textBoxCounter == 3:
                            currentTextBoxElement = arcpy.mapping.ListLayoutElements(reportmxd, "TEXT_ELEMENT", "TextBox3")[0]

                        # Write out header elements...
                        textString = "<FNT size = \"8\"><BOL>Soil ID:</BOL> %s </FNT>\r\n" %(str(key))
                        plain = "Soil ID: %s " %(str(key))

                        for comp in component:
                            if len(comp)==1:
                                comp = str(comp).strip("[]").replace("'","").split(",")
                                for com in comp:
                                    textString = textString + " <BOL>%s</BOL> :%s | "%(com.split(":")[0],com.split(":")[1])
                                    plain = plain + " %s :%s | "%(com.split(":")[0],com.split(":")[1])
                            elif len(comp) ==2:
                                for com in comp:
                                    com = str(com).strip("[]").replace("'","").split(",")
                                for co in com:
                                    textString = textString + " <BOL>%s</BOL> :%s | "%(co.split(":")[0],co.split(":")[1])
                                    plain= plain + " %s :%s | "%(co.split(":")[0].replace("'",""),co.split(":")[1].replace("'",""))
                            else:
                                textString = textString + " <BOL>%s</BOL> :%s | "%(comp.split(":")[0].replace("'",""),comp.split(":")[1].replace("'",""))
                                plain = plain + " %s :%s | "%(comp.split(":")[0].replace("'",""),comp.split(":")[1].replace("'",""))

                        currentTextBoxElement.text = textString
                        soilFieldsKML[key]= plain
                        print len(plain)

                        if textBoxCounter == 3: # we have to write out a full page...
                            # Get reference to the Order Number text element & Change the Order ID on the map to reflect the current Order...
                            OrderNumTextElement = arcpy.mapping.ListLayoutElements(reportmxd, "TEXT_ELEMENT", "OrderIDText")[0]
                            OrderNumTextElement.text = OrderNumText
                            # Current Page Number...
                            pageNumberTextElement = arcpy.mapping.ListLayoutElements(reportmxd, "TEXT_ELEMENT", "PageNumberText")[0]
                            pageNumberTextElement.text = str(pageCounter)
                            # Number of Pages... (STILL TO DO, just put in 'x' for now)...
                            # numberOfPagesTextElement = arcpy.mapping.ListLayoutElements(reportmxd, "TEXT_ELEMENT", "NumberPagesText")[0]
                            # numberOfPagesTextElement.text = "x" #str(pageCounter)
                            # Latitude & Longitude of site point...
                            AddressTextElement = arcpy.mapping.ListLayoutElements(reportmxd, "TEXT_ELEMENT", "AddressText")[0]
                            AddressTextElement.text = AddressString
                            # Buffer Distance
                            bufferDistanceTextElement = arcpy.mapping.ListLayoutElements(reportmxd, "TEXT_ELEMENT", "BufferDistanceText")[0]
                            bufferDistanceTextElement.text = buffer2DistanceValue
                            # write this page to the interim PDF file...

                            if 3*pageCounter< count:
                                nextPagePDF = os.path.join(scratchfolder, "ReportPage" + str(pageCounter) + ".pdf")
                                arcpy.mapping.ExportToPDF(reportmxd, nextPagePDF, "PAGE_LAYOUT") #, 640, 480, 600, "BEST", "RGB", True, "NONE", "RASTERIZE_BITMAP", False, True, "LAYERS_AND_ATTRIBUTES", True, 80)
                                pageCounter = pageCounter + 1
                                textBoxCounter = 1
                                reportmxd = arcpy.mapping.MapDocument(r"\\cabcvan1gis006\GISData\RSC_Ontario\PDFToolboxes\MXD\ERISSoilsReport_new.mxd")
                        elif textBoxCounter < 3:
                            textBoxCounter = textBoxCounter + 1
                        soilBox = soilBox+1

                OrderNumTextElement = arcpy.mapping.ListLayoutElements(reportmxd, "TEXT_ELEMENT", "OrderIDText")[0]
                OrderNumTextElement.text = OrderNumText
                # Current Page Number...
                pageNumberTextElement = arcpy.mapping.ListLayoutElements(reportmxd, "TEXT_ELEMENT", "PageNumberText")[0]
                pageNumberTextElement.text = str(pageCounter)

                AddressTextElement = arcpy.mapping.ListLayoutElements(reportmxd, "TEXT_ELEMENT", "AddressText")[0]
                AddressTextElement.text = AddressString

                bufferDistanceTextElement = arcpy.mapping.ListLayoutElements(reportmxd, "TEXT_ELEMENT", "BufferDistanceText")[0]
                bufferDistanceTextElement.text = buffer2DistanceValue

                nextPagePDF = os.path.join(scratchfolder, "ReportPage" + str(pageCounter) + ".pdf")
                arcpy.mapping.ExportToPDF(reportmxd, nextPagePDF, "PAGE_LAYOUT") #, 640, 480, 600, "BEST", "RGB", True, "NONE", "RASTERIZE_BITMAP", False, True, "LAYERS_AND_ATTRIBUTES", True, 80)
            else:
                nextPagePDF = os.path.join(scratchfolder, "ReportPage"+ str(pageCounter)+".pdf")
                arcpy.mapping.ExportToPDF(reportmxd, nextPagePDF, "PAGE_LAYOUT")
        else:
            OrderNumTextElement = arcpy.mapping.ListLayoutElements(reportmxd, "TEXT_ELEMENT", "OrderIDText")[0]
            OrderNumTextElement.text = OrderNumText
            pageNumberTextElement = arcpy.mapping.ListLayoutElements(reportmxd, "TEXT_ELEMENT", "PageNumberText")[0]
            pageNumberTextElement.text = str(pageCounter)
            AddressTextElement = arcpy.mapping.ListLayoutElements(reportmxd, "TEXT_ELEMENT", "AddressText")[0]
            AddressTextElement.text = AddressString
            bufferDistanceTextElement = arcpy.mapping.ListLayoutElements(reportmxd, "TEXT_ELEMENT", "BufferDistanceText")[0]
            bufferDistanceTextElement.text = buffer2DistanceValue
            nextPagePDF = os.path.join(scratchfolder, "ReportPage"+ str(pageCounter)+".pdf")
            arcpy.mapping.ExportToPDF(reportmxd, nextPagePDF, "PAGE_LAYOUT")

# soil -------------------------------------------------------------------------------------------------------------------------------
        if noSoil==False:
            soilclip = os.path.join(scratch,"soilclip")
            soilclip1 = os.path.join(scratch,"soilclip1")
            soilyr = r"soilclip_lyr"
            mxdname = os.path.join(scratchfolder,'ERISSoilsLayout.mxd')
            mxd = arcpy.mapping.MapDocument(mxdname)
            df = arcpy.mapping.ListDataFrames(mxd,"*")[0]    # the spatial reference here is UTM zone #, need to change to WGS84 Web Mercator
            df.spatialReference = srWGS84
            dfAsFeature = arcpy.Polygon(arcpy.Array([df.extent.lowerLeft, df.extent.lowerRight, df.extent.upperRight, df.extent.upperLeft]),
                                df.spatialReference)    #df.spatialReference is currently UTM. dfAsFeature is a feature, not even a layer
            del df, mxd
            arcpy.Project_management(dfAsFeature, os.path.join(viewertemp,"Extent_soil_WGS84.shp"), srWGS84)
            del dfAsFeature
            arcpy.Clip_analysis(outPolySHP1, os.path.join(viewertemp,"Extent_soil_WGS84.shp"),soilclip)

            if arcpy.Describe(soilclip).spatialReference.name != srWGS84.name:
                arcpy.Project_management(soilclip, soilclip1, srWGS84)
            else:
                soilclip1 =soilclip

            if int(arcpy.GetCount_management(soilclip1).getOutput(0)) != 0:
                arcpy.AddField_management(soilclip1,"Soil", "TEXT", field_length = 5500)
                rows = arcpy.UpdateCursor(soilclip1)

                for row in rows:
                    soilid = row.getValue("POLY_ID")

                    if soilid in soilFieldsKML.keys():
                        row.Soil = soilFieldsKML[soilid]
                    else:
                        row.Soil = 'Unit is not within the search radius'
                    rows.updateRow(row)

                fieldInfo = ""
                fieldList = arcpy.ListFields(soilclip1)

                for field in fieldList:
                    if field.name == 'Soil':
                        fieldInfo = fieldInfo + field.name + " " + "Soil" + " VISIBLE;"
                    else:
                        fieldInfo = fieldInfo + field.name + " " + field.name + " HIDDEN;"
                # print fieldInfo
                arcpy.MakeFeatureLayer_management(soilclip1, soilyr,"", "", fieldInfo[:-1])
                arcpy.ApplySymbologyFromLayer_management(soilyr, os.path.join(r"\\cabcvan1gis006\GISData\RSC_Ontario\PDFToolboxes\LYR",r"soil_new.lyr"))
                arcpy.SaveToLayerFile_management(soilyr, os.path.join(scratchfolder,"soilXX.lyr"), "ABSOLUTE")
                soilyr = arcpy.mapping.Layer(os.path.join(scratchfolder,"soilXX.lyr"))
                soilyr.symbology.valueField = "POLY_ID"
                soilyr.symbology.addAllValues()
                arcpy.LayerToKML_conversion(soilyr, os.path.join(viewerdir_kml,"soilclip.kmz"))
                arcpy.Delete_management(soilyr)
            else:
                print "no soil data to kml"
                arcpy.MakeFeatureLayer_management(soilclip1, soilyr)
                arcpy.LayerToKML_conversion(soilyr, os.path.join(viewerdir_kml,"soilclip_nodata.kmz"))
                arcpy.Delete_management(soilyr)
        else:
            OrderNumTextElement = arcpy.mapping.ListLayoutElements(reportmxd, "TEXT_ELEMENT", "OrderIDText")[0]
            OrderNumTextElement.text = OrderNumText
            pageNumberTextElement = arcpy.mapping.ListLayoutElements(reportmxd, "TEXT_ELEMENT", "PageNumberText")[0]
            pageNumberTextElement.text = str(pageCounter)
            AddressTextElement = arcpy.mapping.ListLayoutElements(reportmxd, "TEXT_ELEMENT", "AddressText")[0]
            AddressTextElement.text = AddressString
            bufferDistanceTextElement = arcpy.mapping.ListLayoutElements(reportmxd, "TEXT_ELEMENT", "BufferDistanceText")[0]
            bufferDistanceTextElement.text = buffer2DistanceValue
            nextPagePDF = os.path.join(scratchfolder, "ReportPage"+ str(pageCounter)+".pdf")
            arcpy.mapping.ExportToPDF(reportmxd, nextPagePDF, "PAGE_LAYOUT")

    elif MapType == "ANSI":
        # Get a referenct to the ANSI Report page template...
        reportmxd = arcpy.mapping.MapDocument(r"\\cabcvan1gis006\GISData\RSC_Ontario\PDFToolboxes\MXD\ERISANSIReport.mxd")
        # Next, Clip the ANSI data to the Buffer 2
        outPolySHP = os.path.join(scratch, "ansishp")

        masterLayer_ansi = arcpy.mapping.Layer(r"\\cabcvan1gis006\GISData\RSC_Ontario\PDFToolboxes\OntarioReports.gdb\ANSI_2017")
        arcpy.SelectLayerByLocation_management(masterLayer_ansi,'intersect',outBuffer2SHP)
        noANSI = False

        if (int((arcpy.GetCount_management(masterLayer_ansi).getOutput(0))) !=0):
            arcpy.Clip_analysis(r"\\cabcvan1gis006\GISData\RSC_Ontario\PDFToolboxes\OntarioReports.gdb\ANSI_2017", outBuffer2SHP, outPolySHP)

            polygonRows = arcpy.SearchCursor(outPolySHP)
            pageCounter = 1
            textBoxCounter = 1

            for currentPolygonRow in polygonRows:
                if textBoxCounter == 1:
                    currentTextBoxElement = arcpy.mapping.ListLayoutElements(reportmxd, "TEXT_ELEMENT", "TextBox1")[0]
                if textBoxCounter == 2:
                    currentTextBoxElement = arcpy.mapping.ListLayoutElements(reportmxd, "TEXT_ELEMENT", "TextBox2")[0]
                if textBoxCounter == 3:
                    currentTextBoxElement = arcpy.mapping.ListLayoutElements(reportmxd, "TEXT_ELEMENT", "TextBox3")[0]
                if textBoxCounter == 4:
                    currentTextBoxElement = arcpy.mapping.ListLayoutElements(reportmxd, "TEXT_ELEMENT", "TextBox4")[0]
                if textBoxCounter == 5:
                    currentTextBoxElement = arcpy.mapping.ListLayoutElements(reportmxd, "TEXT_ELEMENT", "TextBox5")[0]
                if textBoxCounter == 6:
                    currentTextBoxElement = arcpy.mapping.ListLayoutElements(reportmxd, "TEXT_ELEMENT", "TextBox6")[0]
                if textBoxCounter == 7:
                    currentTextBoxElement = arcpy.mapping.ListLayoutElements(reportmxd, "TEXT_ELEMENT", "TextBox7")[0]
                if textBoxCounter == 8:
                    currentTextBoxElement = arcpy.mapping.ListLayoutElements(reportmxd, "TEXT_ELEMENT", "TextBox8")[0]
                # Assemble a header string for the current ANSI details - just putting the name on the first line...
                textString = "<FNT size = \"11\"><BOL>ANSI Name:</BOL> "+ str(currentPolygonRow.ANSI_NAME).replace("&","and") + "</FNT>\r\n" # New line here.
                textString = textString + " <BOL>ID:</BOL> " + str(int(currentPolygonRow.OGF_ID)) + " | "
                textString = textString + " <BOL>Type:</BOL> " + currentPolygonRow.SUBTYPE + " | "
                textString = textString + " <BOL>Significance:</BOL> " + currentPolygonRow.SIGNIF + " | "
                textString = textString + " <BOL>Management Plan:</BOL> " + currentPolygonRow.MGMT_PLAN + " | "
                textString = textString + " <BOL>Area (sqm):</BOL> " + str(currentPolygonRow.SYS_AREA) + " | "
                textString = textString + " <BOL>Comments:</BOL> " + str(currentPolygonRow.GNL_CMT).replace("&","and") + " " # No 'pipe' after last value
                # Write out the assembled textString to the text propert of the current element...
                currentTextBoxElement.text = textString

                if textBoxCounter == 8: # we have to write out a full page...
                    # Get reference to the Order Number text element & Change the Order ID on the map to reflect the current Order...
                    OrderNumTextElement = arcpy.mapping.ListLayoutElements(reportmxd, "TEXT_ELEMENT", "OrderIDText")[0]
                    OrderNumTextElement.text = OrderNumText
                    # Current Page Number...
                    pageNumberTextElement = arcpy.mapping.ListLayoutElements(reportmxd, "TEXT_ELEMENT", "PageNumberText")[0]
                    pageNumberTextElement.text = str(pageCounter)

                    AddressTextElement = arcpy.mapping.ListLayoutElements(reportmxd, "TEXT_ELEMENT", "AddressText")[0]
                    AddressTextElement.text = AddressString
                    # Buffer Distance
                    bufferDistanceTextElement = arcpy.mapping.ListLayoutElements(reportmxd, "TEXT_ELEMENT", "BufferDistanceText")[0]
                    bufferDistanceTextElement.text = buffer2DistanceValue
                    # write this page to the interim PDF file...
                    nextPagePDF = os.path.join(scratchfolder, "ReportPage" + str(pageCounter) + ".pdf")
                    arcpy.mapping.ExportToPDF(reportmxd, nextPagePDF, "PAGE_LAYOUT") #, 640, 480, 600, "BEST", "RGB", True, "NONE", "RASTERIZE_BITMAP", False, True, "LAYERS_AND_ATTRIBUTES", True, 80)
                    # after writing the page, increment the page count, and reset the text box count...
                    pageCounter = pageCounter + 1
                    textBoxCounter = 1
                    # Now, we need a new reference to the Soils Report MXD...
                    reportmxd = arcpy.mapping.MapDocument(r"\\cabcvan1gis006\GISData\RSC_Ontario\PDFToolboxes\MXD\ERISANSIReport.mxd")
                elif textBoxCounter < 8:
                    textBoxCounter = textBoxCounter + 1

            OrderNumTextElement = arcpy.mapping.ListLayoutElements(reportmxd, "TEXT_ELEMENT", "OrderIDText")[0]
            OrderNumTextElement.text = OrderNumText
            # Current Page Number...
            pageNumberTextElement = arcpy.mapping.ListLayoutElements(reportmxd, "TEXT_ELEMENT", "PageNumberText")[0]
            pageNumberTextElement.text = str(pageCounter)

            AddressTextElement = arcpy.mapping.ListLayoutElements(reportmxd, "TEXT_ELEMENT", "AddressText")[0]
            AddressTextElement.text = AddressString
            # Buffer Distance
            bufferDistanceTextElement = arcpy.mapping.ListLayoutElements(reportmxd, "TEXT_ELEMENT", "BufferDistanceText")[0]
            bufferDistanceTextElement.text = buffer2DistanceValue
            # write this page to the interim PDF file...
            nextPagePDF = os.path.join(scratchfolder, "ReportPage" + str(pageCounter) + ".pdf")
            arcpy.mapping.ExportToPDF(reportmxd, nextPagePDF, "PAGE_LAYOUT") #, 640, 480, 600, "BEST", "RGB", True, "NONE", "RASTERIZE_BITMAP", False, True, "LAYERS_AND_ATTRIBUTES", True, 80)
        else:
            noANSI = True
            pageCounter =1
            OrderNumTextElement = arcpy.mapping.ListLayoutElements(reportmxd, "TEXT_ELEMENT", "OrderIDText")[0]
            OrderNumTextElement.text = OrderNumText
            AddressTextElement = arcpy.mapping.ListLayoutElements(reportmxd, "TEXT_ELEMENT", "AddressText")[0]
            AddressTextElement.text = AddressString
            nextPagePDF = os.path.join(scratchfolder, "ReportPage1.pdf")
            arcpy.mapping.ExportToPDF(reportmxd, nextPagePDF, "PAGE_LAYOUT")
# ANSI -----------------------------------------------------------------------------------------------------------------
        if noANSI == False:
            ansiclip = os.path.join(scratch, "ansiclip")
            ansiclip1 = os.path.join(scratch, "ansiclip1")
            ansiclip2 = os.path.join(scratch, "ansiclip_PR")
            ansilyr = r"ansiclip_lyr"
            ansitemplate = os.path.join(r"\\cabcvan1gis006\GISData\RSC_Ontario\PDFToolboxes\LYR",r"ANSI.lyr")
            mxdname = os.path.join(scratchfolder,"ERISANSILayout.mxd")
            mxd = arcpy.mapping.MapDocument(mxdname)
            df = arcpy.mapping.ListDataFrames(mxd,"")[0]    # the spatial reference here is UTM zone #, need to change to WGS84 Web Mercator
            df.spatialReference = srWGS84
            # re-focus using Buffer layer for multipage
            dfAsFeature = arcpy.Polygon(arcpy.Array([df.extent.lowerLeft, df.extent.lowerRight, df.extent.upperRight, df.extent.upperLeft]),
                                df.spatialReference)    #df.spatialReference is currently UTM. dfAsFeature is a feature, not even a layer
            del df, mxd
            ansi_boudnary = os.path.join(scratchfolder,"Extent_ansi_WGS84.shp")
            arcpy.Project_management(dfAsFeature, ansi_boudnary, srWGS84)
            arcpy.Clip_analysis(outPolySHP, ansi_boudnary, ansiclip)

            if arcpy.Describe(ansiclip).spatialReference.name != srWGS84.name:
                arcpy.Project_management(ansiclip, ansiclip1, srWGS84)
            else:
                ansiclip1 =ansiclip
            del dfAsFeature

            if int(arcpy.GetCount_management(ansiclip1).getOutput(0)) != 0:
                arcpy.AddField_management(ansi_boudnary,"SIGNIF", "TEXT", "", "", "15", "", "NULLABLE", "NON_REQUIRED", "")
                arcpy.AddField_management(ansiclip1,"ANSI", "TEXT", field_length = 1500)
                rows = arcpy.UpdateCursor(ansiclip1)
                for row in rows:
                    textString = "ANSI Name: "+ str(row.ANSI_NAME).replace("&","and")  # New line here.
                    textString = textString + " ID: " + str(int(row.OGF_ID)) + " | "
                    textString = textString + " Type: " + row.SUBTYPE + " | "
                    textString = textString + " Significance: " + row.SIGNIF + " | "
                    textString = textString + " Management Plan: " + row.MGMT_PLAN + " | "
                    textString = textString + " Area (sqm): " + str(row.SYS_AREA) + " | "
                    textString = textString + " Comments: " + str(row.GNL_CMT).replace("&","and") + " " # No 'pipe' after last value
                    row.ANSI = textString
                    rows.updateRow(row)
                arcpy.Union_analysis([ansiclip1,ansi_boudnary],ansiclip2)

                fieldInfo = ""
                fieldList = arcpy.ListFields(ansiclip2)

                for field in fieldList:
                   if field.name == 'ANSI':
                        fieldInfo = fieldInfo + field.name + " " + "ANSI_NAME" + " VISIBLE;"
                   else:
                        fieldInfo = fieldInfo + field.name + " " + field.name + " HIDDEN;"
                arcpy.MakeFeatureLayer_management(ansiclip2, ansilyr, "", "", fieldInfo[:-1])
                arcpy.ApplySymbologyFromLayer_management(ansilyr, ansitemplate)
                # arcpy.SaveToLayerFile_management(ansilyr, os.path.join(scratchfolder,"ansiXX.lyr"), "ABSOLUTE")
                arcpy.LayerToKML_conversion(ansilyr, os.path.join(viewerdir_kml,"ansiclip.kmz"))
                arcpy.Delete_management(ansilyr)
            else:
                print "no ansi data, no kml to folder"
                arcpy.MakeFeatureLayer_management(ansiclip1, ansilyr)
                # arcpy.SaveToLayerFile_management(ansilyr, os.path.join(scratchfolder,"ansiXX.lyr"), "ABSOLUTE")
                arcpy.LayerToKML_conversion(ansilyr, os.path.join(viewerdir_kml,"ansiclip_nodata.kmz"))
                arcpy.Delete_management(ansilyr)

    elif MapType == "Bedrock Geology":
        reportmxd = arcpy.mapping.MapDocument(r"\\cabcvan1gis006\GISData\RSC_Ontario\PDFToolboxes\MXD\ERISBedrockGeologyReport.mxd")
        # Next, Clip the Bedrock Geology data to the Buffer 2
        outPolySHP = os.path.join(scratchfolder, outPolyFileName)
        arcpy.Clip_analysis(r"\\cabcvan1gis006\GISData\RSC_Ontario\PDFToolboxes\OntarioReports.gdb\BedrockGeologyPolygons", outBuffer2SHP, outPolySHP)
        # Now, create a search cursor for the output polygons...
        polygonRows = arcpy.SearchCursor(outPolySHP)

        pageCounter = 1
        textBoxCounter = 1
        # start looping through the rows in the clipped shapefile...
        # There are seven (7) text boxes on the Bedrock Geology Report Page....
        for currentPolygonRow in polygonRows:
            if textBoxCounter == 1:
                currentTextBoxElement = arcpy.mapping.ListLayoutElements(reportmxd, "TEXT_ELEMENT", "TextBox1")[0]
            if textBoxCounter == 2:
                currentTextBoxElement = arcpy.mapping.ListLayoutElements(reportmxd, "TEXT_ELEMENT", "TextBox2")[0]
            if textBoxCounter == 3:
                currentTextBoxElement = arcpy.mapping.ListLayoutElements(reportmxd, "TEXT_ELEMENT", "TextBox3")[0]
            if textBoxCounter == 4:
                currentTextBoxElement = arcpy.mapping.ListLayoutElements(reportmxd, "TEXT_ELEMENT", "TextBox4")[0]
            if textBoxCounter == 5:
                currentTextBoxElement = arcpy.mapping.ListLayoutElements(reportmxd, "TEXT_ELEMENT", "TextBox5")[0]
            if textBoxCounter == 6:
                currentTextBoxElement = arcpy.mapping.ListLayoutElements(reportmxd, "TEXT_ELEMENT", "TextBox6")[0]
            if textBoxCounter == 7:
                currentTextBoxElement = arcpy.mapping.ListLayoutElements(reportmxd, "TEXT_ELEMENT", "TextBox7")[0]
            # Assemble a header string for the current Bedrock Geology details - just putting the name on the first line...
            textString = "<FNT size = \"11\"><BOL>ID:</BOL> " + str(currentPolygonRow.GEOLOGY_ID) + " |  <BOL>Unit Name: </BOL>" + currentPolygonRow.UNITNAME_P + " | </FNT>\r\n" # New line here.
            textString = textString + " <BOL>Type (All):</BOL> " + currentPolygonRow.TYPE_ALL + " | "
            textString = textString + " <BOL>Type (Primary):</BOL> " + currentPolygonRow.TYPE_P + " | "
            textString = textString + " <BOL>Type (Secondary):</BOL> " + currentPolygonRow.TYPE_S + " | "
            textString = textString + " <BOL>Type (Tertiary):</BOL> " + currentPolygonRow.TYPE_T + " | "
            textString = textString + " <BOL>Rock Type (Primary):</BOL> " + currentPolygonRow.ROCKTYPE_P + " | "
            textString = textString + " <BOL>Strata (Primary):</BOL> " + currentPolygonRow.STRAT_P + " | "
            tempStringValue = currentPolygonRow.SUPEREON_P
            tempStringValue = tempStringValue.replace("<", "&lt;")
            tempStringValue = tempStringValue.replace(">", "&gt;")
            textString = textString + " <BOL>Super Eon (Primary):</BOL> " + tempStringValue + " | "
            textString = textString + " <BOL>Eon (Primary):</BOL> " + currentPolygonRow.EON_P.replace('<','&lt;') + " | "
            textString = textString + " <BOL>Era (Primary):</BOL> " + currentPolygonRow.ERA_P.replace('<','&lt;') + " | "
            textString = textString + " <BOL>Period (Primary):</BOL> " + currentPolygonRow.PERIOD_P.replace('<','&lt;') + " | "
            textString = textString + " <BOL>Epoch (Primary):</BOL> " + currentPolygonRow.EPOCH_p.replace('<','&lt;') + " | "
            textString = textString + " <BOL>Province (Primary):</BOL> " + currentPolygonRow.PROVINCE_P.replace('<','&lt;') + " " # no 'pipe' on last value
            # Write out the assembled textString to the text propert of the current element...
            currentTextBoxElement.text = textString

            if textBoxCounter == 7: # we have to write out a full page...
                # Get reference to the Order Number text element & Change the Order ID on the map to reflect the current Order...
                OrderNumTextElement = arcpy.mapping.ListLayoutElements(reportmxd, "TEXT_ELEMENT", "OrderIDText")[0]
                OrderNumTextElement.text = OrderNumText
                # Current Page Number...
                pageNumberTextElement = arcpy.mapping.ListLayoutElements(reportmxd, "TEXT_ELEMENT", "PageNumberText")[0]
                pageNumberTextElement.text = str(pageCounter)

                AddressTextElement = arcpy.mapping.ListLayoutElements(reportmxd, "TEXT_ELEMENT", "AddressText")[0]
                AddressTextElement.text = AddressString
                # Buffer Distance
                bufferDistanceTextElement = arcpy.mapping.ListLayoutElements(reportmxd, "TEXT_ELEMENT", "BufferDistanceText")[0]
                bufferDistanceTextElement.text = buffer2DistanceValue
                # write this page to the interim PDF file...
                nextPagePDF = os.path.join(scratchfolder, "ReportPage" + str(pageCounter) + ".pdf")
                arcpy.mapping.ExportToPDF(reportmxd, nextPagePDF, "PAGE_LAYOUT") #, 640, 480, 600, "BEST", "RGB", True, "NONE", "RASTERIZE_BITMAP", False, True, "LAYERS_AND_ATTRIBUTES", True, 80)
                # after writing the page, increment the page count, and reset the text box count...
                pageCounter = pageCounter + 1
                textBoxCounter = 1
                # Now, we need a new reference to the Soils Report MXD...
                reportmxd = arcpy.mapping.MapDocument(r"\\cabcvan1gis006\GISData\RSC_Ontario\PDFToolboxes\MXD\ERISBedrockGeologyReport.mxd")
            elif textBoxCounter < 7:
                textBoxCounter = textBoxCounter + 1

        OrderNumTextElement = arcpy.mapping.ListLayoutElements(reportmxd, "TEXT_ELEMENT", "OrderIDText")[0]
        OrderNumTextElement.text = OrderNumText
        # Current Page Number...
        pageNumberTextElement = arcpy.mapping.ListLayoutElements(reportmxd, "TEXT_ELEMENT", "PageNumberText")[0]
        pageNumberTextElement.text = str(pageCounter)

        AddressTextElement = arcpy.mapping.ListLayoutElements(reportmxd, "TEXT_ELEMENT", "AddressText")[0]
        AddressTextElement.text = AddressString
        # Buffer Distance
        bufferDistanceTextElement = arcpy.mapping.ListLayoutElements(reportmxd, "TEXT_ELEMENT", "BufferDistanceText")[0]
        bufferDistanceTextElement.text = buffer2DistanceValue
        # write this page to the interim PDF file...
        nextPagePDF = os.path.join(scratchfolder, "ReportPage" + str(pageCounter) + ".pdf")
        arcpy.mapping.ExportToPDF(reportmxd, nextPagePDF, "PAGE_LAYOUT") #, 640, 480, 600, "BEST", "RGB", True, "NONE", "RASTERIZE_BITMAP", False, True, "LAYERS_AND_ATTRIBUTES", True, 80)
        # Bedrock Geology reports have a static metadata page describing all the field values.  We need to generate the interim PDF here
        bedrockmetadataMXD = arcpy.mapping.MapDocument(r"\\cabcvan1gis006\GISData\RSC_Ontario\PDFToolboxes\MXD\ERISBedrockGeologyMetadata.mxd")
        bedrockmetadataPDF = os.path.join(scratchfolder, "BedrockMetaDataPage.pdf")
        arcpy.mapping.ExportToPDF(bedrockmetadataMXD, bedrockmetadataPDF, "PAGE_LAYOUT") #, 640, 480, 600, "BEST", "RGB", True, "NONE", "RASTERIZE_BITMAP", False, True, "LAYERS_AND_ATTRIBUTES", True, 80)

# bedrock geology -------------------------------------------------------------------------------------------------------------------------------
        geologyclipB = os.path.join(scratch, "geologyclipB")
        geologyclipB1 = os.path.join(scratch, "geologyclipB1")
        geologyB_lyr = 'geologyclipB_lyr'
        mxdname = os.path.join(scratchfolder,'ERISBedrockGeologyLayout.mxd')
        mxd = arcpy.mapping.MapDocument(mxdname)
        df = arcpy.mapping.ListDataFrames(mxd,"*")[0]    # the spatial reference here is UTM zone #, need to change to WGS84 Web Mercator
        df.spatialReference = srWGS84
        dfAsFeature = arcpy.Polygon(arcpy.Array([df.extent.lowerLeft, df.extent.lowerRight, df.extent.upperRight, df.extent.upperLeft]),df.spatialReference)    #df.spatialReference is currently UTM. dfAsFeature is a feature, not even a layer
        del df, mxd
        arcpy.Project_management(dfAsFeature, os.path.join(viewertemp,"Extent_geolB_WGS84.shp"), srWGS84)
        arcpy.Clip_analysis(r"\\cabcvan1gis006\GISData\RSC_Ontario\PDFToolboxes\OntarioReports.gdb\BedrockGeologyPolygons", os.path.join(viewertemp,"Extent_geolB_WGS84.shp"), geologyclipB,"0.000001 DecimalDegrees")
   
        if arcpy.Describe(geologyclipB).spatialReference.name != srWGS84.name:
            arcpy.Project_management(geologyclipB, geologyclipB1, srWGS84)
        else:
            geologyclipB1 =geologyclipB
        del dfAsFeature

        if int(arcpy.GetCount_management(geologyclipB1).getOutput(0)) != 0:
            arcpy.AddField_management(geologyclipB1,"Bedrock", "TEXT", "", "", "1500", "", "NULLABLE", "NON_REQUIRED", "")
            field1=''
            polygonRows = arcpy.UpdateCursor(geologyclipB1)

            for currentPolygonRow in polygonRows:
                textString = "ID: " + str(currentPolygonRow.GEOLOGY_ID) + " |  Unit Name: " + currentPolygonRow.UNITNAME_P + " | " # New line here.
                textString = textString + " Type (All): " + currentPolygonRow.TYPE_ALL + " | "
                textString = textString + " Type (Primary): " + currentPolygonRow.TYPE_P + " | "
                textString = textString + " Type (Secondary): " + currentPolygonRow.TYPE_S + " | "
                textString = textString + " Type (Tertiary): " + currentPolygonRow.TYPE_T + " | "
                textString = textString + " Rock Type (Primary): " + currentPolygonRow.ROCKTYPE_P + " | "
                textString = textString + " Strata (Primary): " + currentPolygonRow.STRAT_P + " | "
                tempStringValue = currentPolygonRow.SUPEREON_P
                tempStringValue = tempStringValue.replace("<", "&lt;")
                tempStringValue = tempStringValue.replace(">", "&gt;")
                textString = textString + " Super Eon (Primary): " + tempStringValue + " | "
                textString = textString + " Eon (Primary): " + currentPolygonRow.EON_P.replace('<','&lt;') + " | "
                textString = textString + " Era (Primary): " + currentPolygonRow.ERA_P.replace('<','&lt;') + " | "
                textString = textString + " Period (Primary): " + currentPolygonRow.PERIOD_P.replace('<','&lt;') + " | "
                textString = textString + " Epoch (Primary): " + currentPolygonRow.EPOCH_p.replace('<','&lt;') + " | "
                textString = textString + " Province (Primary): " + currentPolygonRow.PROVINCE_P.replace('<','&lt;') + " " # no 'pipe' on last value

                currentPolygonRow.Bedrock = textString
                field1 = "Bedrock"
                polygonRows.updateRow(currentPolygonRow)

            fieldInfo = ""
            fieldList = arcpy.ListFields(geologyclipB1)

            for field in fieldList:
                if field.name == field1:
                    fieldInfo = fieldInfo + field.name + " " + "Bedrock Geology:" + " VISIBLE;"
                else:
                    fieldInfo = fieldInfo + field.name + " " + field.name + " HIDDEN;"
            # print fieldInfo
            arcpy.MakeFeatureLayer_management(geologyclipB1, geologyB_lyr, "", "", fieldInfo[:-1])
            arcpy.ApplySymbologyFromLayer_management(geologyB_lyr, os.path.join(r"\\cabcvan1gis006\GISData\RSC_Ontario\PDFToolboxes\LYR",r"geology_Bedrock.lyr"))
            # arcpy.SaveToLayerFile_management(geologyB_lyr, os.path.join(scratchfolder,"geoB.lyr"), "ABSOLUTE")
            arcpy.LayerToKML_conversion(geologyB_lyr, os.path.join(viewerdir_kml,"geologyclipB.kmz"))
            arcpy.Delete_management(geologyB_lyr)
        else:
            print "no geology data to kml"
            arcpy.MakeFeatureLayer_management(geologyclipB1, geologyB_lyr)
            arcpy.LayerToKML_conversion(geologyB_lyr, os.path.join(viewerdir_kml,"geologyclipB_nodata.kmz"))
            arcpy.Delete_management(geologyB_lyr)

    elif MapType == "Surficial Geology":
        reportmxd = arcpy.mapping.MapDocument(r"\\cabcvan1gis006\GISData\RSC_Ontario\PDFToolboxes\MXD\ERISSurfaceGeologyReport.mxd")
        # Next, Clip the Bedrock Geology data to the Buffer 2
        outPolySHP = os.path.join(scratchfolder, outPolyFileName)
        arcpy.Clip_analysis(r"\\cabcvan1gis006\GISData\RSC_Ontario\PDFToolboxes\OntarioReports.gdb\SurficialGeologyPolygons", outBuffer2SHP, outPolySHP)
        # Now, create a search cursor for the output polygons...
        polygonRows = arcpy.SearchCursor(outPolySHP)
        # Initialize the counter...
        # currentPolygonRow = polygonRows.next()
        # Initialize the Page Count and Text Box count...
        pageCounter = 1
        textBoxCounter = 1
        # start looping through the rows in the clipped shapefile...
        # There are five (5) text boxes on the Surface Geology Report Page....
        for currentPolygonRow in polygonRows:
            if textBoxCounter == 1:
                currentTextBoxElement = arcpy.mapping.ListLayoutElements(reportmxd, "TEXT_ELEMENT", "TextBox1")[0]
            if textBoxCounter == 2:
                currentTextBoxElement = arcpy.mapping.ListLayoutElements(reportmxd, "TEXT_ELEMENT", "TextBox2")[0]
            if textBoxCounter == 3:
                currentTextBoxElement = arcpy.mapping.ListLayoutElements(reportmxd, "TEXT_ELEMENT", "TextBox3")[0]
            if textBoxCounter == 4:
                currentTextBoxElement = arcpy.mapping.ListLayoutElements(reportmxd, "TEXT_ELEMENT", "TextBox4")[0]
            if textBoxCounter == 5:
                currentTextBoxElement = arcpy.mapping.ListLayoutElements(reportmxd, "TEXT_ELEMENT", "TextBox5")[0]

            # Assemble a header string for the current Bedrock Geology details - just putting the name on the first line...
            textString = "<FNT size = \"11\"><BOL>ID:</BOL> " + str(currentPolygonRow.SRFGEO_ID) + " |  <BOL>Unit Name: </BOL>" + currentPolygonRow.GEOLOGIC_D + " | </FNT>\r\n" # New line here.
            # Now, assemble a string for the details for the current Bedrock Geology polygon...
            # NOTE Need to fix this later - some of these are truncated field names as we're going out to shapefiles for scratchfolder.
            # need to change code to use File GDB for scratchfolder...
            #
            # Note: Commented out some of the fields below as per e-mail from Diana Feb 16, 2012.
            textString = textString + " <BOL>Deposit Type Code:</BOL> " + currentPolygonRow.DEPOSIT_TY + " | "
            textString = textString + " <BOL>Deposit Age:</BOL> " + currentPolygonRow.DEPOSIT_AG + " | "
            textString = textString + " <BOL>Map Number:</BOL> " + currentPolygonRow.MAP_NUM + " | "
            textString = textString + " <BOL>Map Name:</BOL> " + currentPolygonRow.MAP_NAME + " | "
            textString = textString + " <BOL>Source Map Scale:</BOL> " + currentPolygonRow.SOURCE_MAP + " | "
            #textString = textString + " <BOL>Origin ID::</BOL> " + currentPolygonRow.ORIG_ID + " | "
            #textString = textString + " <BOL>New ID:</BOL> " + currentPolygonRow.NEW_ID + " | "
            #textString = textString + " <BOL>Single ID:</BOL> " + currentPolygonRow.SINGLE_NEW + " | "
            textString = textString + " <BOL>Primary Material:</BOL> " + currentPolygonRow.PRIM_MAT + " | "
            #textString = textString + " <BOL>Single Primary Matarial:</BOL> " + currentPolygonRow.SINGLE_PRI + " | "
            textString = textString + " <BOL>Primary Material Modifier:</BOL> " + currentPolygonRow.P_MAT_MOD + " | "
            #textString = textString + " <BOL>Single Primary Material Modifier:</BOL> " + currentPolygonRow.SINGLE_PMA + " | "
            textString = textString + " <BOL>Secondary Material:</BOL> " + currentPolygonRow.SEC_MAT + " | "
            textString = textString + " <BOL>Primary General:</BOL> " + currentPolygonRow.PRIM_GEN + " | "
            #textString = textString + " <BOL>Single Primary General:</BOL> " + currentPolygonRow.SINGLE_P_1 + " | "
            textString = textString + " <BOL>Primary General Modifier:</BOL> " + currentPolygonRow.PRIM_GEN_M + " | "
            #textString = textString + " <BOL>Single Primary General Modifier:</BOL> " + currentPolygonRow.SINGLE_PGE + " | "
            textString = textString + " <BOL>Veneer:</BOL> " + currentPolygonRow.VENEER + " | "
            textString = textString + " <BOL>Episode:</BOL> " + currentPolygonRow.EPISODE + " | "
            textString = textString + " <BOL>Sub Episode:</BOL> " + currentPolygonRow.SUBEPISODE + " | "
            textString = textString + " <BOL>Phase:</BOL> " + currentPolygonRow.PHASE + " | "
            textString = textString + " <BOL>Stratus Modifier:</BOL> " + currentPolygonRow.STRAT_MOD + " | "
            textString = textString + " <BOL>Provenance:</BOL> " + currentPolygonRow.PROVENANCE + " | "
            textString = textString + " <BOL>Carbon Content:</BOL> " + currentPolygonRow.CARB_CONTE + " | "
            textString = textString + " <BOL>Formation:</BOL> " + currentPolygonRow.FORMATION + " | "
            textString = textString + " <BOL>Permeability:</BOL> " + currentPolygonRow.PERMEABILI + " | "
            # testing the 'replace' funtion to change "<" to "&lt;" and ">" to "&gt;"...
            tempStringValue = currentPolygonRow.MATERIAL_D
            tempStringValue = tempStringValue.replace("<", "&lt;")
            tempStringValue = tempStringValue.replace(">", "&gt;")
            textString = textString + " <BOL>Material Description:</BOL> " + tempStringValue + " " # no 'pipe' on last value
            # Write out the assembled textString to the text propert of the current element...
            currentTextBoxElement.text = textString
            # When we get here, we have written the current Surface Geology to the current text box on
            # on the current page.  Now we have to test to see if we need a new page. This is the case if
            # the textBoxCounter value is now 5 (i.e. we have just written to TextBox5, so there are no more on
            # the page...
            if textBoxCounter == 5: # we have to write out a full page...
                # Get reference to the Order Number text element & Change the Order ID on the map to reflect the current Order...
                OrderNumTextElement = arcpy.mapping.ListLayoutElements(reportmxd, "TEXT_ELEMENT", "OrderIDText")[0]
                OrderNumTextElement.text = OrderNumText
                # Current Page Number...
                pageNumberTextElement = arcpy.mapping.ListLayoutElements(reportmxd, "TEXT_ELEMENT", "PageNumberText")[0]
                pageNumberTextElement.text = str(pageCounter)
                # Number of Pages... (STILL TO DO, just put in 'x' for now)...
                # numberOfPagesTextElement = arcpy.mapping.ListLayoutElements(reportmxd, "TEXT_ELEMENT", "NumberPagesText")[0]
                # numberOfPagesTextElement.text = "x" #str(pageCounter)
                # Latitude & Longitude of site point...
                AddressTextElement = arcpy.mapping.ListLayoutElements(reportmxd, "TEXT_ELEMENT", "AddressText")[0]
                AddressTextElement.text = AddressString
                # Buffer Distance
                bufferDistanceTextElement = arcpy.mapping.ListLayoutElements(reportmxd, "TEXT_ELEMENT", "BufferDistanceText")[0]
                bufferDistanceTextElement.text = buffer2DistanceValue
                # write this page to the interim PDF file...
                nextPagePDF = os.path.join(scratchfolder, "ReportPage" + str(pageCounter) + ".pdf")
                arcpy.mapping.ExportToPDF(reportmxd, nextPagePDF, "PAGE_LAYOUT") #, 640, 480, 600, "BEST", "RGB", True, "NONE", "RASTERIZE_BITMAP", False, True, "LAYERS_AND_ATTRIBUTES", True, 80)
                # after writing the page, increment the page count, and reset the text box count...
                pageCounter = pageCounter + 1
                textBoxCounter = 1
                # Now, we need a new reference to the Surface Geology Report MXD...
                reportmxd = arcpy.mapping.MapDocument(r"\\cabcvan1gis006\GISData\RSC_Ontario\PDFToolboxes\MXD\ERISSurfaceGeologyReport.mxd")
            elif textBoxCounter < 5:
                textBoxCounter = textBoxCounter + 1

        # Next Surface Geology Map unit loops back here..
        # Once here, we can assume we have written all the rows for for all the Surface Geology units.  Since the
        # The last page will not get to the 10th text box, we have to write out the final page where it is at...
        # Get reference to the Order Number text element & Change the Order ID on the map to reflect the current Order...
        OrderNumTextElement = arcpy.mapping.ListLayoutElements(reportmxd, "TEXT_ELEMENT", "OrderIDText")[0]
        OrderNumTextElement.text = OrderNumText
        # Current Page Number...
        pageNumberTextElement = arcpy.mapping.ListLayoutElements(reportmxd, "TEXT_ELEMENT", "PageNumberText")[0]
        pageNumberTextElement.text = str(pageCounter)
        # Number of Pages... STILL TO DO, just put in 'x' for now...
        # numberOfPagesTextElement = arcpy.mapping.ListLayoutElements(reportmxd, "TEXT_ELEMENT", "NumberPagesText")[0]
        # numberOfPagesTextElement.text = "x" #str(pageCounter)
        # Latitude & Longitude of site point...
        AddressTextElement = arcpy.mapping.ListLayoutElements(reportmxd, "TEXT_ELEMENT", "AddressText")[0]
        AddressTextElement.text = AddressString
        # Buffer Distance
        bufferDistanceTextElement = arcpy.mapping.ListLayoutElements(reportmxd, "TEXT_ELEMENT", "BufferDistanceText")[0]
        bufferDistanceTextElement.text = buffer2DistanceValue
        # write this page to the interim PDF file...
        nextPagePDF = os.path.join(scratchfolder, "ReportPage" + str(pageCounter) + ".pdf")
        arcpy.mapping.ExportToPDF(reportmxd, nextPagePDF, "PAGE_LAYOUT") #, 640, 480, 600, "BEST", "RGB", True, "NONE", "RASTERIZE_BITMAP", False, True, "LAYERS_AND_ATTRIBUTES", True, 80)
        # Surface Geology reports have a static metadata page describing all the field values.  We need to generate the interim PDF here
        surfacemetadataMXD = arcpy.mapping.MapDocument(r"\\cabcvan1gis006\GISData\RSC_Ontario\PDFToolboxes\MXD\ERISSurfaceGeologyMetadata.mxd")
        surfacemetadataPDF = os.path.join(scratchfolder, "SurfaceMetaDataPage.pdf")
        arcpy.mapping.ExportToPDF(surfacemetadataMXD, surfacemetadataPDF, "PAGE_LAYOUT") #, 640, 480, 600, "BEST", "RGB", True, "NONE", "RASTERIZE_BITMAP", False, True, "LAYERS_AND_ATTRIBUTES", True, 80)

# surficial geology -------------------------------------------------------------------------------------------------------------------------------
        geologyclipS = os.path.join(scratch, "geologyclipS")
        geologyclipS1 = os.path.join(scratch, "geologyclipS1")
        geologyS_lyr = 'geologyclipS_lyr'
        mxdname = os.path.join(scratchfolder,'ERISSurfaceGeologyLayout.mxd')
        mxd = arcpy.mapping.MapDocument(mxdname)
        df = arcpy.mapping.ListDataFrames(mxd,"*")[0]       # the spatial reference here is UTM zone #, need to change to WGS84 Web Mercator
        df.spatialReference = srWGS84
        dfAsFeature = arcpy.Polygon(arcpy.Array([df.extent.lowerLeft, df.extent.lowerRight, df.extent.upperRight, df.extent.upperLeft]),
                            df.spatialReference)            # df.spatialReference is currently UTM. dfAsFeature is a feature, not even a layer
        del df, mxd
        arcpy.Project_management(dfAsFeature, os.path.join(viewertemp,"Extent_geolS_WGS84.shp"), srWGS84)
        del dfAsFeature
        arcpy.Clip_analysis(r"\\cabcvan1gis006\GISData\RSC_Ontario\PDFToolboxes\OntarioReports.gdb\SurficialGeologyPolygons", os.path.join(viewertemp,"Extent_geolS_WGS84.shp"),geologyclipS,"0.000001 DecimalDegrees")
      
        if arcpy.Describe(geologyclipS).spatialReference.name != srWGS84.name:
            arcpy.Project_management(geologyclipS, geologyclipS1, srWGS84)
        else:
            geologyclipS1 =geologyclipS

        if int(arcpy.GetCount_management(geologyclipS1).getOutput(0)) != 0:
            arcpy.AddField_management(geologyclipS1,"Surficial", "TEXT", "", "", "1500", "", "NULLABLE", "NON_REQUIRED", "")
            field1 = ''
            polygonRows = arcpy.UpdateCursor(geologyclipS1)

            for currentPolygonRow in polygonRows:
                textString = "ID: " + str(currentPolygonRow.SRFGEO_ID) + " |  Unit Name: " + currentPolygonRow.GEOLOGIC_DEPOSIT + " | " # New line here.
                textString = textString + " Deposit Type Code: " + currentPolygonRow.DEPOSIT_TYPE_COD + " | "
                textString = textString + " Deposit Age: " + currentPolygonRow.DEPOSIT_AGE + " | "
                textString = textString + " Map Number: " + currentPolygonRow.MAP_NUM + " | "
                textString = textString + " Map Name: " + currentPolygonRow.MAP_NAME + " | "
                textString = textString + " Source Map Scale: " + currentPolygonRow.SOURCE_MAP_SCALE + " | "
                textString = textString + " Primary Material: " + currentPolygonRow.PRIM_MAT + " | "
                textString = textString + " Primary Material Modifier: " + currentPolygonRow.P_MAT_MOD + " | "
                textString = textString + " Secondary Material: " + currentPolygonRow.SEC_MAT + " | "
                textString = textString + " Primary General: " + currentPolygonRow.PRIM_GEN + " | "
                textString = textString + " Primary General Modifier: " + currentPolygonRow.PRIM_GEN_MOD + " | "
                textString = textString + " Veneer: " + currentPolygonRow.VENEER + " | "
                textString = textString + " Episode: " + currentPolygonRow.EPISODE + " | "
                textString = textString + " Sub Episode: " + currentPolygonRow.SUBEPISODE + " | "
                textString = textString + " Phase: " + currentPolygonRow.PHASE + " | "
                textString = textString + " Stratus Modifier: " + currentPolygonRow.STRAT_MOD + " | "
                textString = textString + " Provenance: " + currentPolygonRow.PROVENANCE + " | "
                textString = textString + " Carbon Content: " + currentPolygonRow.CARB_CONTENT + " | "
                textString = textString + " Formation: " + currentPolygonRow.FORMATION + " | "
                textString = textString + " Permeability: " + currentPolygonRow.PERMEABILITY + " | "
                tempStringValue = currentPolygonRow.MATERIAL_DESCRIP
                tempStringValue = tempStringValue.replace("<", "&lt;")
                tempStringValue = tempStringValue.replace(">", "&gt;")
                textString = textString + " Material Description: " + tempStringValue + " " # no 'pipe' on last value
                currentPolygonRow.Surficial = textString
                field1 = "Surficial"
                polygonRows.updateRow(currentPolygonRow)

            fieldInfo = ""
            fieldList = arcpy.ListFields(geologyclipS1)

            for field in fieldList:
                if field.name == field1:
                    fieldInfo = fieldInfo + field.name + " " + "Surficial Geology:" + " VISIBLE;"
                else:
                    fieldInfo = fieldInfo + field.name + " " + field.name + " HIDDEN;"
            # print fieldInfo
            arcpy.MakeFeatureLayer_management(geologyclipS1, geologyS_lyr, "", "", fieldInfo[:-1])
            arcpy.ApplySymbologyFromLayer_management(geologyS_lyr,os.path.join(r"\\cabcvan1gis006\GISData\RSC_Ontario\PDFToolboxes\LYR",r"geology_Surficial.lyr"))
            # arcpy.SaveToLayerFile_management(geologyS_lyr, os.path.join(scratchfolder,"geoS.lyr"), "ABSOLUTE")
            arcpy.LayerToKML_conversion(geologyS_lyr, os.path.join(viewerdir_kml,geologyS_lyr+".kmz"))
            arcpy.Delete_management(geologyS_lyr)
        else:
            print "no geology data to kml"
            arcpy.MakeFeatureLayer_management(geologyclipS1, geologyS_lyr)
            arcpy.LayerToKML_conversion(geologyS_lyr, os.path.join(viewerdir_kml,"geologyclipS_nodata.kmz"))
            arcpy.Delete_management(geologyS_lyr)
    # ----------------
    # Create the final Combined PDF file...
    #
    # ExportToPDF (map_document, out_pdf, {data_frame}, {df_export_width}, {df_export_height}, {resolution}, {image_quality},
    #              {colorspace}, {compress_vectors}, {image_compression}, {picture_symbol}, {convert_markers}, {embed_fonts},
    #              {layers_attributes}, {georef_info}, {jpeg_compression_quality})
    # ----------------
    # Generate the Merged Report PDF...
    # outputPDF = os.path.join(scratchfolder, 'FullReport.pdf')
    # Get reference to the first page... - Make sure this matches the line below - see <<MUST MATCH>>
    deliverfolder = os.path.join(scratchfolder, OrderNumText+'_RSC_Order')

    if not os.path.exists(deliverfolder):
        os.makedirs(deliverfolder)

    outputPDF = arcpy.mapping.PDFDocumentCreate(os.path.join(deliverfolder, OrderNumText + "_CA_" + MapType+ ".pdf"))
    # Add the completed map Layout PDF as the first page...
    outputPDF.appendPages(outputLayoutPDF)
    # we need to to extra work here if the Map Type is Sois, ANSI, Suface or Bedrock Geology
    # because there are extra pages.. - there will or or many 'ReportX.pdf' files!!!
    # Python lacks a if this "OR" that logic, so there is some redundancy below...
    # if the map type is "OBM" or "Physiography", none of these get called, as there is only one page (the map)
    # for these two map types.
    if MapType == "Soils":                          # soils is unique as it has the metadata page...
        outputPageCounter = 1                       #s tart at two, because if there is only one page, the while loop won't be extecuted...
        while outputPageCounter <= pageCounter:     #only append if the page counter is more than 1
            outputPDF.appendPages(os.path.join(scratchfolder, "ReportPage" + str(outputPageCounter) + ".pdf"))
            outputPageCounter = outputPageCounter + 1
    elif MapType == "ANSI":                         # "ANSI", "Surficial Geology" and "Bedrock Geology" are all the same, but no way to isolate...
        outputPageCounter = 1                       # start at two, because if there is only one page, the while loop won't be extecuted...
        while outputPageCounter <= pageCounter:     # only append if the page counter is more than 1
            outputPDF.appendPages(os.path.join(scratchfolder, "ReportPage" + str(outputPageCounter) + ".pdf"))
            outputPageCounter = outputPageCounter + 1
    elif MapType == "Surficial Geology":            # "ANSI", "Surficial Geology" and "Bedrock Geology" are all the same, but no way to isolate...
        outputPageCounter = 1                       # tart at two, because if there is only one page, the while loop won't be extecuted...
        while outputPageCounter <= pageCounter:     # only append if the page counter is more than 1
            outputPDF.appendPages(os.path.join(scratchfolder, "ReportPage" + str(outputPageCounter) + ".pdf"))
            outputPageCounter = outputPageCounter + 1
        outputPDF.appendPages(os.path.join(scratchfolder, "SurfaceMetaDataPage.pdf")) # for Surface Geology, add the metadata page last
    elif MapType == "Bedrock Geology":              # "ANSI", "Surficial Geology" and "Bedrock Geology" are all the same, but no way to isolate...
        outputPageCounter = 1                       # start at two, because if there is only one page, the while loop won't be extecuted...
        while outputPageCounter <= pageCounter:     # only append if the page counter is more than 1
            outputPDF.appendPages(os.path.join(scratchfolder, "ReportPage" + str(outputPageCounter) + ".pdf"))
            outputPageCounter = outputPageCounter + 1
        outputPDF.appendPages(os.path.join(scratchfolder, "BedrockMetaDataPage.pdf")) # for Bedrock Geology, add the metadata page last

    outputPDF.saveAndClose() # close the PDF

if os.path.exists(os.path.join(viewer_path, OrderNumText+"_rsckml")):
    shutil.rmtree(os.path.join(viewer_path, OrderNumText+"_rsckml"))

shutil.copytree(os.path.join(scratchfolder, OrderNumText+"_rsckml"), os.path.join(viewer_path, OrderNumText+"_rsckml"))
url = upload_link + "RSCKMLUpload?ordernumber=" + OrderNumText
urllib.urlopen(url)

if os.path.exists(os.path.join(viewer_path, OrderNumText+"_rscimage")):
    shutil.rmtree(os.path.join(viewer_path, OrderNumText+"_rscimage"))

shutil.copytree(os.path.join(scratchfolder, OrderNumText+"_rscimage"), os.path.join(viewer_path, OrderNumText+"_rscimage"))
url = upload_link + "RSCImageUpload?ordernumber=" + OrderNumText
urllib.urlopen(url)
# Lastly, send the completed report back as the output parameter - Make sure this matches the line above - see <<MUST MATCH>>

if ['OBM']==MapTypesArray or ['OBM','ANSI']==MapTypesArray:
        reportcheck_path = os.path.join(reportcheck_path,"TopographicMaps")
        if len(os.listdir(deliverfolder))==1:
            if os.path.exists( os.path.join(deliverfolder, OrderNumText + "_CA_OBM.pdf")):
                output = os.path.join(deliverfolder, OrderNumText + "_CA_OBM.pdf")
            else:
                raise Exception("no file found %s"%(os.path.join(deliverfolder, OrderNumText + "_CA_OBM.pdf")))
        else:
            output = os.path.join(scratchfolder, OrderNumText + "_CA_OBM.zip")
            shutil.make_archive(os.path.join(scratchfolder, OrderNumText + "_CA_OBM"), 'zip', deliverfolder)
elif len(MapTypesArray) > 1:
    reportcheck_path = os.path.join(reportcheck_path,"RSCMaps")
    # zip to a file
    output = os.path.join(scratchfolder, OrderNumText + "_CA_RSC.zip")
    zipf = zipfile.ZipFile(output, 'w')
    zipdir(deliverfolder, zipf)
    zipf.close()
elif len(MapTypesArray)==1:
    reportcheck_path = os.path.join(reportcheck_path,"RSCMaps")
    output = os.path.join(deliverfolder, OrderNumText + "_CA_" + MapType+ ".pdf")

shutil.copy(output,reportcheck_path)
arcpy.SetParameterAsText(7, output)      # note somehow the pdf1 in the sub folder doesn't properly return. So have to copy and use pdf2

# print("Final RSC zip directory: " + (reportcheck_path + "\\" + OrderNumText + "_CA_RSC.zip"))
# print("DONE")