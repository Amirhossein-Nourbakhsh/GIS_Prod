#-------------------------------------------------------------------------------
# Name:        module1
# Purpose:
#
# Author:      cchen
#
# Created:     27/04/2017
# Copyright:   (c) cchen 2017
# Licence:     <your licence>
#-------------------------------------------------------------------------------

import shutil, csv
import cx_Oracle, urllib, glob,zipfile,contextlib
import arcpy, os, numpy
from datetime import datetime
import getDirectionText
import gc, time
import traceback
from numpy import gradient
from numpy import arctan2, arctan, sqrt
import PSR_CAN_config
import xml.etree.ElementTree as ET
import json

def addBuffertoMxd(bufferName,thedf):    # note: buffer is a shapefile, the name doesn't contain .shp
    bufferLayer = arcpy.mapping.Layer(PSR_CAN_config.bufferlyrfile)
    bufferLayer.replaceDataSource(scratchfolder,"SHAPEFILE_WORKSPACE",bufferName)
    arcpy.mapping.AddLayer(thedf,bufferLayer,"Top")
    thedf.extent = bufferLayer.getSelectedExtent(False)
    thedf.scale = thedf.scale * 1.1

def addOrdergeomtoMxd(ordergeomName, thedf):
    orderGeomLayer = arcpy.mapping.Layer(orderGeomlyrfile)
    orderGeomLayer.replaceDataSource(scratchfolder,"SHAPEFILE_WORKSPACE",ordergeomName)
    arcpy.mapping.AddLayer(thedf,orderGeomLayer,"Top")

def calMapkey(fclass):
    arcpy.env.OverWriteOutput = True

    temp = arcpy.CopyFeatures_management(fclass, r'in_memory/temp1')
    cur = arcpy.UpdateCursor(temp,"Distance = 0" ,"","Dist_cent; Distance; MapKeyLoc; MapKeyNo", 'Dist_cent A; Source A')

    lastMapkeyloc = 0
    row = cur.next()
    if row is not None:
        last = row.getValue('Dist_cent') # the last value in field A
        #print str(row.getValue('Distance')) +", " + str(last)
        row.setValue('mapkeyloc', 1)
        row.setValue('mapkeyno', 1)

        cur.updateRow(row)
        run = 1 # how many values in this run
        count = 1 # how many runs so far, including the current one

        # the for loop should begin from row 2, since
        # cur.next() has already been called once.
        for row in cur:
            current = row.getValue('Dist_cent')
            #print str(row.getValue('Distance')) + ", " + str(current)
            if current == last:
                run += 1
            else:
                run = 1
                count += 1
            row.setValue('mapkeyloc', count)
            row.setValue('mapkeyno', run)
            cur.updateRow(row)

            last = current
        lastMapkeyloc = count
        # release the layer from locks
    del row, cur

    cur = arcpy.UpdateCursor(temp,"Distance > 0" ,"","Distance; MapKeyLoc; MapKeyNo", 'Distance A; Source A')
    row = cur.next()

    if row is not None:
        last = row.getValue('Distance') # the last value in field A
        # print "Part 2 start " + str(last)+ "   lastmaykeyloc is " + str(lastMapkeyloc)
        row.setValue('mapkeyloc', lastMapkeyloc + 1)
        row.setValue('mapkeyno', 1)


        cur.updateRow(row)
        run = 1 # how many values in this run
        count = lastMapkeyloc + 1 # how many runs so far, including the current one

        # the for loop should begin from row 2, since
        # cur.next() has already been called once.
        for row in cur:
            current = row.getValue('Distance')
            #print "Part 2 start " + str(last)+ "   lastmapkeyloc is " + str(lastMapkeyloc)
            if current == last:
                run += 1
            else:
                run = 1
                count += 1
            row.setValue('mapkeyloc', count)
            row.setValue('mapkeyno', run)
            cur.updateRow(row)

            last = current

        # release the layer from locks
    del row, cur

    cur = arcpy.UpdateCursor(temp, "", "", 'MapKeyLoc; mapKeyNo; MapkeyTot', 'MapKeyLoc D; mapKeyNo D')
    row = cur.next()
    
    if row is not None:
        last = row.getValue('mapkeyloc') # the last value in field A
        max= 1
        row.setValue('mapkeytot', max)
        cur.updateRow(row)

        for row in cur:
            current = row.getValue('mapkeyloc')

            if current < last:
                max= 1
            else:
                max= 0
            row.setValue('mapkeytot', max)
            cur.updateRow(row)

            last = current

    # release the layer from locks
    del row, cur
    arcpy.CopyFeatures_management(temp, fclass)

def getElevation(dataset,fields):
    pntlist={}
    with arcpy.da.SearchCursor(dataset,fields) as uc:
        for row in uc:
            pntlist[row[2]]=(row[0],row[1])
    del uc

    params={}
    params['XYs']=pntlist
    params = urllib.urlencode(params)
    inhouse_esri_geocoder = r"https://gisserverprod.glaciermedia.ca/arcgis/rest/services/GPTools_temp/pntElevation2/GPServer/pntElevation2/execute?env%3AoutSR=&env%3AprocessSR=&returnZ=false&returnM=false&f=pjson"
    f = urllib.urlopen(inhouse_esri_geocoder,params)
    results =  json.loads(f.read())
    result = eval( results['results'][0]['value'])

    check_field = arcpy.ListFields(dataset,"Elevation")
    if len(check_field)==0:
        arcpy.AddField_management(dataset, "Elevation", "DOUBLE", "12", "6", "", "", "NULLABLE", "NON_REQUIRED", "")
    with arcpy.da.UpdateCursor(dataset,["Elevation"]) as uc:
        for row in uc:
            row[0]=-999
            uc.updateRow(row)
    del uc

    with arcpy.da.UpdateCursor(dataset,['Elevation',fields[-1]]) as uc:
        for row in uc:
            row[0]= result[row[1]]
            uc.updateRow(row)
    del row
    return dataset

try:
# ------------------------------------------------------------------------------------------------------------------------
   OrderIDText = arcpy.GetParameterAsText(0)
   scratch = arcpy.env.scratchGDB
   scratchfolder = arcpy.env.scratchWorkspace

    # OrderIDText =''
    # OrderNumText = r"21021000152"
    # scratchfolder = os.path.join(r"C:\Users\awong\Downloads\PSR_SCRATCHY", OrderNumText)
    # if not os.path.exists(scratchfolder):
    #     os.mkdir(scratchfolder)
    # arcpy.CreateFileGDB_management(scratchfolder, "scratch.gdb")
    scratch = os.path.join(scratchfolder,"scratch.gdb")   # for tables to make Querytable

    # for regular .shp etc.
# ------------------------------------------------------------------------------------------------------------------------
    try:
        con = cx_Oracle.connect(PSR_CAN_config.connectionString)
        cur = con.cursor()

        # GET ORDER_ID FROM ORDER_NUM
        if OrderIDText == "":
            cur.execute("SELECT * FROM ERIS.PSR_AUDIT WHERE ORDER_ID IN (select order_id from orders where order_num = '" + str(OrderNumText) + "')")
            result = cur.fetchall()
            OrderIDText = str(result[0][0]).strip()
            print("Order ID: " + OrderIDText)

        cur.execute("select order_num, address1, city, provstate from orders where order_id =" + OrderIDText)
        t = cur.fetchone()

        OrderNumText = str(t[0])
        AddressText = str(t[1])+","+str(t[2])+","+str(t[3])
        Prov = str(t[3])

        cur.execute("select geometry_type, geometry, radius_type  from eris_order_geometry where order_id =" + OrderIDText)
        t = cur.fetchone()

        cur.callproc('eris_psr.ClearOrder', (OrderIDText,))

        OrderType = str(t[0])
        OrderCoord = eval(str(t[1]))
        RadiusType = str(t[2])
    finally:
        cur.close()
        con.close()

# OUTPUT ------------------------------------------------------------------------------------------------------------------------
    outputjpg_topo = os.path.join(scratchfolder,OrderNumText+'_CA_TOPO.jpg')
    outputjpg_relief = os.path.join(scratchfolder,OrderNumText+'_CA_RELIEF.jpg')
    outputjpg_wetland = os.path.join(scratchfolder,OrderNumText+'_CA_WETL.jpg')
    outputjpg_soil =os.path.join(scratchfolder,OrderNumText+'_CA_SOIL.jpg')
    outputjpg_geolS =os.path.join(scratchfolder,OrderNumText+'_CA_Surficial_GEOL.jpg')
    outputjpg_geolB =os.path.join(scratchfolder,OrderNumText+'_CA_Bedrock_GEOL.jpg')
    outputjpg_wells = os.path.join(scratchfolder,OrderNumText+'_CA_WELLS.jpg')
    outputjpg_ansi = os.path.join(scratchfolder,OrderNumText+'_CA_ANSI.jpg')

    print"--print- starting " + time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
    searchRadius = {}
    try:
        con = cx_Oracle.connect(PSR_CAN_config.connectionString)
        cur = con.cursor()

        cur.execute("select DS_OID, SEARCH_RADIUS, REPORT_SOURCE from order_radius_psr where order_id =" + str(OrderIDText))
        items = cur.fetchall()

        for t in items:
            dsoid = t[0]
            radius = t[1]
            reportsource = t[2]

            searchRadius[str(dsoid)] = float(radius)

    finally:
        cur.close()
        con.close()

    dsoid_wells = []
    for radius in searchRadius:
        if radius in PSR_CAN_config.TopoList:
            bufferDist_topo = str(searchRadius[radius]) + ' KILOMETERS'
        elif radius in PSR_CAN_config.WetlandList:
            bufferDist_wetland = str(searchRadius[radius]) + ' KILOMETERS'
        elif radius in PSR_CAN_config.BedrockList+PSR_CAN_config.SurficialList:
            bufferDist_geol = str(searchRadius[radius]) + ' KILOMETERS'
        elif radius in PSR_CAN_config.SoilList:
            bufferDist_soil = str(searchRadius[radius]) + ' KILOMETERS'
        elif radius in PSR_CAN_config.RadonList:
            bufferDist_radon = str(searchRadius[radius]) + ' KILOMETERS'
        elif radius in PSR_CAN_config.ANSI_ON:
            bufferDist_ansi = str(searchRadius[radius]) + ' KILOMETERS'
        else:
            dsoid_wells.append(radius)

    dsoid_wells_maxradius = dsoid_wells[0]
    for radius in dsoid_wells:
        print("-----")
        print(radius)
        print searchRadius[radius]
        if searchRadius[radius] >= searchRadius[dsoid_wells_maxradius]:
            dsoid_wells_maxradius = radius

    arcpy.env.overwriteOutput = True
    arcpy.env.OverWriteOutput = True

    erisid = 0

    point = arcpy.Point()
    array = arcpy.Array()
    sr = arcpy.SpatialReference()
    sr.factoryCode = 4269  # requires input geometry is in 4269
    sr.XYTolerance = .00000001
    sr.scaleFactor = 2000
    sr.create()
    featureList = []
    for feature in OrderCoord:
        # For each coordinate pair, set the x,y properties and add to the Array object.
        for coordPair in feature:
            point.X = coordPair[0]
            point.Y = coordPair[1]
            sr.setDomain (point.X, point.X, point.Y, point.Y)
            array.add(point)
        if OrderType.lower()== 'point':
            feat = arcpy.Multipoint(array, sr)
        elif OrderType.lower() =='polyline':
            feat  = arcpy.Polyline(array, sr)
        else :
            feat = arcpy.Polygon(array,sr)
        array.removeAll()

        # Append to the list of Polygon objects
        featureList.append(feat)

    orderGeometry= os.path.join(scratchfolder,"orderGeometry.shp")
    arcpy.CopyFeatures_management(featureList, orderGeometry)
    del featureList
    srNAD83 = arcpy.SpatialReference(4269)  #NAD83
    arcpy.DefineProjection_management(orderGeometry, srNAD83)

    arcpy.AddField_management(orderGeometry, "xCentroid", "DOUBLE", 18, 11)
    arcpy.AddField_management(orderGeometry, "yCentroid", "DOUBLE", 18, 11)

    xExpression = u'!SHAPE.CENTROID.X!'
    yExpression = u'!SHAPE.CENTROID.Y!'

    arcpy.CalculateField_management(orderGeometry, 'xCentroid', xExpression, "PYTHON_9.3")
    arcpy.CalculateField_management(orderGeometry, 'yCentroid', yExpression, "PYTHON_9.3")

    arcpy.AddField_management(orderGeometry, "UTM", "TEXT", "", "", "1500", "", "NULLABLE", "NON_REQUIRED", "")
    arcpy.CalculateUTMZone_cartography(orderGeometry, 'UTM')
    UT= arcpy.SearchCursor(orderGeometry)
    UTMvalue = ''
    Lat_Y = 0
    Lon_X = 0
    for row in UT:
        UTMvalue = str(row.getValue('UTM'))[41:43]
        Lat_Y = row.getValue('yCentroid')
        Lon_X = row.getValue('xCentroid')
    del UT
    out_coordinate_system = os.path.join(PSR_CAN_config.connectionPath+'/', r"projections/NAD1983/NAD1983UTMZone"+UTMvalue+"N.prj")

    orderGeometryPR = os.path.join(scratchfolder, "ordergeoNamePR.shp")
    arcpy.Project_management(orderGeometry, orderGeometryPR, out_coordinate_system)
    arcpy.AddField_management(orderGeometryPR, "xCenUTM", "DOUBLE", 18, 11)
    arcpy.AddField_management(orderGeometryPR, "yCenUTM", "DOUBLE", 18, 11)

    xExpression = '!SHAPE.CENTROID.X!'
    yExpression = '!SHAPE.CENTROID.Y!'

    arcpy.CalculateField_management(orderGeometryPR, 'xCenUTM', xExpression, "PYTHON_9.3")
    arcpy.CalculateField_management(orderGeometryPR, 'yCenUTM', yExpression, "PYTHON_9.3")

    del point
    del array

    if OrderType.lower()== 'point':
        orderGeomlyrfile = PSR_CAN_config.orderGeomlyrfile_point
    elif OrderType.lower() =='polyline':
        orderGeomlyrfile = PSR_CAN_config.orderGeomlyrfile_polyline
    else:
        orderGeomlyrfile = PSR_CAN_config.orderGeomlyrfile_polygon

    spatialRef = arcpy.SpatialReference(out_coordinate_system)

    # determine if needs to be multipage
    # according to Raf: will be multipage if line is over 1/4 mile, or polygon is over 1 sq miles
    # need to check the extent of the geometry
    geomExtent = arcpy.Describe(orderGeometryPR).extent
    multipage_topo = False
    multipage_relief = False
    multipage_wetland = False
    multipage_ansi = False
    multipage_flood = False
    multipage_geology = False
    multipage_soil = False
    multipage_wells = False

    gridsize = "2 KILOMETERS"
    if geomExtent.width > 1300 or geomExtent.height > 1300:
        multipage_wetland = True
        multipage_flood = True
        multipage_ansi = True
        multipage_geology = True
        multipage_soil = True
        multipage_topo = True
        multipage_relief = True
        multipage_topo = True
        multipage_wells = True
    if geomExtent.width > 500 or geomExtent.height > 500:
        multipage_topo = True
        multipage_relief = True
        multipage_topo = True
        multipage_wells = True

# 1 current Topo map, no attributes ------------------------------------------------------------------------------------------------------
    if 'bufferDist_topo' in locals():
        print "--- starting Topo section " + time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
        bufferSHP_topo = os.path.join(scratchfolder,"buffer_topo.shp")
        arcpy.Buffer_analysis(orderGeometryPR, bufferSHP_topo, bufferDist_topo)

        point = arcpy.Point()
        array = arcpy.Array()
        featureList = []

        width = arcpy.Describe(bufferSHP_topo).extent.width/2
        height = arcpy.Describe(bufferSHP_topo).extent.height/2

        if (width/height > 7/7):    #7/7 now since adjusted the frame to square
            # wider shape
            height = width/7*7
        else:
            # longer shape
            width = height/7*7
        xCentroid = (arcpy.Describe(bufferSHP_topo).extent.XMax + arcpy.Describe(bufferSHP_topo).extent.XMin)/2
        yCentroid = (arcpy.Describe(bufferSHP_topo).extent.YMax + arcpy.Describe(bufferSHP_topo).extent.YMin)/2

        if multipage_topo == True:
            width = width + 6400     #add 2 miles to each side, for multipage
            height = height + 6400   #add 2 miles to each side, for multipage

        Topo = ''
        if Prov == 'BC':
            masterLayer_topoBC = arcpy.mapping.Layer(PSR_CAN_config.masterlyr_topoBC)
            arcpy.SelectLayerByLocation_management(masterLayer_topoBC,'intersect',bufferSHP_topo)
            if(int((arcpy.GetCount_management(masterLayer_topoBC).getOutput(0))) !=0):
                Topo = 'BC'
            else: Topo = 'N'

        if Prov == 'ON':
            masterLayer_topoON = arcpy.mapping.Layer(PSR_CAN_config.masterlyr_topoON)
            arcpy.SelectLayerByLocation_management(masterLayer_topoON,'intersect',bufferSHP_topo)
            if(int((arcpy.GetCount_management(masterLayer_topoON).getOutput(0))) !=0):
                Topo = 'ON'
            else: Topo = 'N'

        if Prov == 'NS':
           masterLayer_topoNS = arcpy.mapping.Layer(PSR_CAN_config.masterlyr_topoNS)
           arcpy.SelectLayerByLocation_management(masterLayer_topoNS,'intersect',bufferSHP_topo)
           if(int((arcpy.GetCount_management(masterLayer_topoNS).getOutput(0))) !=0):
               Topo = 'NS'
           else: Topo = 'N'

        if Topo != Prov:
            masterLayer_toporama = arcpy.mapping.Layer(PSR_CAN_config.masterlyr_toporama)
            arcpy.SelectLayerByLocation_management(masterLayer_toporama,'intersect',bufferSHP_topo)
            if(int((arcpy.GetCount_management(masterLayer_toporama).getOutput(0))) !=0):
                Topo = 'CA'
            else: Topo = 'N'

# ------------------------------------------------------------------------------------------------------------------------
        if Topo == 'CA':
            topofolder =os.path.join(scratchfolder,OrderNumText+"_topotemp")
            if not os.path.exists(topofolder):
                os.makedirs(topofolder)
            cellNOs = []
            rows = arcpy.SearchCursor(masterLayer_toporama)    # loop through the selected records
            for row in rows:
                cellNO = (row.getValue("IDENTIF")).strip()
                # print cellNO
                cellNOs.append(cellNO)
            del row
            del rows

            for cellNO in cellNOs:
                myPath = os.path.join(PSR_CAN_config.tifdir_toporama,str(cellNO[0:3]),str(cellNO[3:4]),"toporama_"+str(cellNO).lower()+"_utm.zip")
                # unzip, use the tif and extract metadata
                with contextlib.closing(zipfile.ZipFile(myPath,"r")) as z:
                    z.extractall(topofolder)
            masterLayer_toporama= None
            mxdfile_topo = PSR_CAN_config.mxdfile_topoCA
            if multipage_topo == True:
                mxdMMfile_topo = PSR_CAN_config.mxdMMfile_topoCA
        elif Topo == 'BC':
            topotifs = []
            topofolder =os.path.join(scratchfolder,OrderNumText+"_topotemp")
            if not os.path.exists(topofolder):
                os.makedirs(topofolder)
            rows = arcpy.SearchCursor(masterLayer_topoBC)    # loop through the selected records
            for row in rows:
                topotifs.append(str(row.path)[:-4])
            del row
            del rows
            for topo in topotifs:
                shutil.copy(topo+".tif",topofolder)
                shutil.copy(topo+".tfw",topofolder)
                shutil.copy(topo+".txt",topofolder)
            masterLayer_topoBC= None
            mxdfile_topo = PSR_CAN_config.mxdfile_topoCA
            if multipage_topo == True:
                mxdMMfile_topo = PSR_CAN_config.mxdMMfile_topoCA
        elif Topo == 'ON':
            masterLayer_topoON= None
            mxdfile_topo = PSR_CAN_config.mxdfile_topoON
            if multipage_topo == True:
                mxdMMfile_topo = PSR_CAN_config.mxdMMfile_topoON
        elif Topo == 'NS':
            masterLayer_topoON= None
            mxdfile_topo = PSR_CAN_config.mxdfile_topoNS
            if multipage_topo == True:
                mxdMMfile_topo = PSR_CAN_config.mxdMMfile_topoNS

# Topo mxd to JPG ------------------------------------------------------------------------------------------------------------------------
        mxd_topo = arcpy.mapping.MapDocument(mxdfile_topo)
        df_topo = arcpy.mapping.ListDataFrames(mxd_topo,"*")[0]
        df_topo.spatialReference = spatialRef
        if multipage_topo == True:
            mxdMM_topo = arcpy.mapping.MapDocument(mxdMMfile_topo)
            dfMM_topo = arcpy.mapping.ListDataFrames(mxdMM_topo,"*")[0]
            dfMM_topo.spatialReference = spatialRef
        # add text
        AddressTextE= arcpy.mapping.ListLayoutElements(mxd_topo, "TEXT_ELEMENT", "AddressText")[0]
        AddressTextE.text = "Address: " + AddressText + ""
        orderSourceTextE = arcpy.mapping.ListLayoutElements(mxd_topo, "TEXT_ELEMENT", "DataSourceText")[0]
##        if Topo == 'CA':
##            orderSourceTextE.text = "Data source: Toporama (1:50K) by Natural Resource Canada.Publication date: 2013-07-19"
##        el
        if Topo =='BC':
            orderSourceTextE.text = "Data source: GeoBC of the Ministry of Forests,Lands,and Natural Resource Operations."
        elif Topo =='ON':
            orderSourceTextE.text =r"Data source: Ontario Base Mapping (OBM) by Ontario Ministry of Natural Resources."
        elif Topo =='NS':
            orderSourceTextE.text =r"Data source: Nova Scotia Topographic Database (NSTDB)."
        # add site
        addBuffertoMxd("buffer_topo",df_topo)
        addOrdergeomtoMxd("ordergeoNamePR", df_topo)

        topofiles = []
        if Topo =='CA' or Topo =='BC':
            os.chdir(topofolder)
            for tiffile in glob.glob("*.tif"):
                if tiffile !='':
                    topofiles.append(os.path.join(topofolder,tiffile))
                    topolayer = arcpy.mapping.Layer(PSR_CAN_config.topolyrfile)
                    topolayer.replaceDataSource(topofolder, "RASTER_WORKSPACE", tiffile)
                    arcpy.mapping.AddLayer(df_topo, topolayer, "BOTTOM")
                    if multipage_topo == True:
                        arcpy.mapping.AddLayer(dfMM_topo, topolayer, "BOTTOM")
            df_topo.scale = df_topo.scale * 1.1
        elif Topo == 'ON':
            newLayerBuffer2 = arcpy.mapping.Layer(PSR_CAN_config.bufferlyrfile)
            newLayerBuffer2.replaceDataSource(scratchfolder, "SHAPEFILE_WORKSPACE", "buffer_topo")

            for lyr in arcpy.mapping.ListLayers(mxd_topo, "", df_topo):
                if lyr.name == "Buffer2":
                    arcpy.mapping.RemoveLayer(df_topo, lyr)
            df_topo.panToExtent(newLayerBuffer2.getExtent())
            df_topo.scale = df_topo.scale * 1.1

        elif Topo =='NS':
            newLayerBuffer2 = arcpy.mapping.Layer(PSR_CAN_config.bufferlyrfile)
            newLayerBuffer2.replaceDataSource(scratchfolder, "SHAPEFILE_WORKSPACE", "buffer_topo")
            df_topo.panToExtent(newLayerBuffer2.getExtent())
            df_topo.scale = df_topo.scale * 1.1

        arcpy.RefreshTOC()

        if multipage_topo == False:
            arcpy.mapping.ExportToJPEG(mxd_topo, outputjpg_topo, "PAGE_LAYOUT", resolution=200, jpeg_quality=100)
            if not os.path.exists(os.path.join(PSR_CAN_config.report_path, 'PSRmaps', OrderNumText)):
                os.mkdir(os.path.join(PSR_CAN_config.report_path, 'PSRmaps', OrderNumText))
            shutil.copy(outputjpg_topo, os.path.join(PSR_CAN_config.report_path, 'PSRmaps', OrderNumText))

            mxd_topo.saveACopy(os.path.join(scratchfolder,"mxd_topo.mxd"))
            del mxd_topo
            del df_topo

        else:     # multipage
            gridlr = "gridlr_topo"   # gdb feature class doesn't work, could be a bug. So use .shp
            gridlrshp = os.path.join(scratch, gridlr)
            arcpy.GridIndexFeatures_cartography(gridlrshp, bufferSHP_topo, "", "", "", gridsize, gridsize)  #note the tool takes featureclass name only, not the full path

            # part 1: the overview map
            # add grid layer
            gridLayer = arcpy.mapping.Layer(PSR_CAN_config.gridlyrfile)
            gridLayer.replaceDataSource(scratch,"FILEGDB_WORKSPACE","gridlr_topo")
            arcpy.mapping.AddLayer(df_topo,gridLayer,"Top")

            df_topo.extent = gridLayer.getExtent()
            df_topo.scale = df_topo.scale * 1.1

            mxd_topo.saveACopy(os.path.join(scratchfolder, "mxd_topo.mxd"))
            arcpy.mapping.ExportToJPEG(mxd_topo, outputjpg_topo, "PAGE_LAYOUT", 480, 640, 200, "False", "24-BIT_TRUE_COLOR", 100)
            if not os.path.exists(os.path.join(PSR_CAN_config.report_path, 'PSRmaps', OrderNumText)):
                os.mkdir(os.path.join(PSR_CAN_config.report_path, 'PSRmaps', OrderNumText))
            shutil.copy(outputjpg_topo, os.path.join(PSR_CAN_config.report_path, 'PSRmaps', OrderNumText))
            del mxd_topo
            del df_topo

            # part 2: the data driven pages
            page = 1
            page = int(arcpy.GetCount_management(gridlrshp).getOutput(0))  + page
            gridlayerMM = arcpy.mapping.ListLayers(mxdMM_topo,"Grid" ,dfMM_topo)[0]
            gridlayerMM.replaceDataSource(scratch, "FILEGDB_WORKSPACE","gridlr_topo")
            arcpy.CalculateAdjacentFields_cartography(gridlrshp, 'PageNumber')
            addBuffertoMxd("buffer_topo",dfMM_topo)
            addOrdergeomtoMxd("ordergeoNamePR", dfMM_topo)
            mxdMM_topo.saveACopy(os.path.join(scratchfolder, "mxdMM_topo.mxd"))

            for i in range(1,int(arcpy.GetCount_management(gridlrshp).getOutput(0))+1):
                arcpy.SelectLayerByAttribute_management(gridlayerMM, "NEW_SELECTION", ' "PageNumber" =  ' + str(i))
                dfMM_topo.extent = gridlayerMM.getSelectedExtent(True)
                dfMM_topo.scale = dfMM_topo.scale * 1.1

                arcpy.SelectLayerByAttribute_management(gridlayerMM, "CLEAR_SELECTION")

                yearTextE = arcpy.mapping.ListLayoutElements(mxdMM_topo, "TEXT_ELEMENT", "MainTitleText")[0]
                yearTextE.text = "Topographic Map - Page " + str(i)
                yearTextE.elementPositionX = 0.4959
                # ADD text
                sourceTextE = arcpy.mapping.ListLayoutElements(mxdMM_topo, "TEXT_ELEMENT", "DataSourceText")[0]
                if Topo == 'CA':
                    sourceTextE.text = "Data source: Toporama (1:50K) by Natural Resource Canada. "
                elif Topo =='BC':
                    sourceTextE.text = "Data source: GeoBC of the Ministry of Forests,Lands,and Natural Resource Operations."
                elif Topo =='ON':
                    sourceTextE.text =r"Data source: Ontario Base Mapping (OBM) by Ontario Ministry of Natural Resources."
                elif Topo =='NS':
                    sourceTextE.text =r"Data source: Nova Scotia Topographic Database (NSTDB)."

                arcpy.RefreshTOC()
                arcpy.mapping.ExportToJPEG(mxdMM_topo, outputjpg_topo[0:-4]+str(i)+".jpg", "PAGE_LAYOUT", 480, 640, 200, "False", "24-BIT_TRUE_COLOR", 100)
                if not os.path.exists(os.path.join(PSR_CAN_config.report_path, 'PSRmaps', OrderNumText)):
                    os.mkdir(os.path.join(PSR_CAN_config.report_path, 'PSRmaps', OrderNumText))
                shutil.copy(outputjpg_topo[0:-4]+str(i)+".jpg", os.path.join(PSR_CAN_config.report_path, 'PSRmaps', OrderNumText))

            del mxdMM_topo
            del dfMM_topo

# 2 shaded relief map ------------------------------------------------------------------------------------------------------------------------
        print "--- starting relief " + time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
        mxd_relief = arcpy.mapping.MapDocument(PSR_CAN_config.mxdfile_relief)
        df_relief = arcpy.mapping.ListDataFrames(mxd_relief,"*")[0]
        df_relief.spatialReference = spatialRef

        point = arcpy.Point()
        array = arcpy.Array()
        featureList = []

        addBuffertoMxd("buffer_topo",df_relief)
        addOrdergeomtoMxd("ordergeoNamePR", df_relief)
        # locate and add relevant shadedrelief tiles
        width = arcpy.Describe(bufferSHP_topo).extent.width/2
        height = arcpy.Describe(bufferSHP_topo).extent.height/2

        if (width/height > 5/4.4):
            # wider shape
            height = width/5*4.4
        else:
            # longer shape
            width = height/4.4*5

        xCentroid = (arcpy.Describe(bufferSHP_topo).extent.XMax + arcpy.Describe(bufferSHP_topo).extent.XMin)/2
        yCentroid = (arcpy.Describe(bufferSHP_topo).extent.YMax + arcpy.Describe(bufferSHP_topo).extent.YMin)/2

        if multipage_topo == True:
            width = width + 6400     # add 2 miles to each side, for multipage
            height = height + 6400   # add 2 miles to each side, for multipage

        point.X = xCentroid-width
        point.Y = yCentroid+height
        array.add(point)
        point.X = xCentroid+width
        point.Y = yCentroid+height
        array.add(point)
        point.X = xCentroid+width
        point.Y = yCentroid-height
        array.add(point)
        point.X = xCentroid-width
        point.Y = yCentroid-height
        array.add(point)
        point.X = xCentroid-width
        point.Y = yCentroid+height
        array.add(point)
        feat = arcpy.Polygon(array,spatialRef)
        array.removeAll()
        featureList.append(feat)
        clipFrame_relief = os.path.join(scratchfolder, "clipFrame_relief.shp")
        arcpy.CopyFeatures_management(featureList, clipFrame_relief)

        masterLayer_relief = arcpy.mapping.Layer(PSR_CAN_config.masterlyr_shadedrelief)
        arcpy.SelectLayerByLocation_management(masterLayer_relief,'intersect',clipFrame_relief)
        # print "after selectLayerByLocation "+time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
        relief = ''
        img_selected = []
        if(int((arcpy.GetCount_management(masterLayer_relief).getOutput(0))) ==0):
            print "NO records selected"
            masterLayer_relief = None
            relief = 'N'

        else:
            rows = arcpy.SearchCursor(masterLayer_relief)    # loop through the selected records
            for row in rows:
                img_selected.append(row.hillshade)
            del row
            del rows
            masterLayer_relief = None
            # print "Before adding data sources" + time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
            for img in img_selected:
                reliefLayer = arcpy.mapping.Layer(PSR_CAN_config.relieflyrfile)
                shutil.copy(os.path.join(PSR_CAN_config.path_shadedrelief,img),scratchfolder)
                reliefLayer.replaceDataSource(scratchfolder,"RASTER_WORKSPACE",img)
                reliefLayer.name = img[:-4]
                arcpy.mapping.AddLayer(df_relief, reliefLayer, "BOTTOM")
                reliefLayer = None

        arcpy.RefreshActiveView()

        if multipage_relief == False:
            mxd_relief.saveACopy(os.path.join(scratchfolder,"mxd_relief.mxd"))
            arcpy.mapping.ExportToJPEG(mxd_relief, outputjpg_relief, "PAGE_LAYOUT", 480, 640, 200, "False", "24-BIT_TRUE_COLOR", 100)
            if not os.path.exists(os.path.join(PSR_CAN_config.report_path, 'PSRmaps', OrderNumText)):
                os.mkdir(os.path.join(PSR_CAN_config.report_path, 'PSRmaps', OrderNumText))
            shutil.copy(outputjpg_relief, os.path.join(PSR_CAN_config.report_path, 'PSRmaps', OrderNumText))

            del mxd_relief
            del df_relief
        else:     #multipage
            gridlr = "gridlr_relief"   #gdb feature class doesn't work, could be a bug. So use .shp
            gridlrshp = os.path.join(scratch, gridlr)
            arcpy.GridIndexFeatures_cartography(gridlrshp, bufferSHP_topo, "", "", "", gridsize, gridsize)  #note the tool takes featureclass name only, not the full path
            # part 1: the overview map
            # add grid layer
            gridLayer = arcpy.mapping.Layer(PSR_CAN_config.gridlyrfile)
            gridLayer.replaceDataSource(scratch,"FILEGDB_WORKSPACE","gridlr_relief")
            arcpy.mapping.AddLayer(df_relief,gridLayer,"Top")

            df_relief.extent = gridLayer.getExtent()
            df_relief.scale = df_relief.scale * 1.1

            mxd_relief.saveACopy(os.path.join(scratchfolder, "mxd_relief.mxd"))
            arcpy.mapping.ExportToJPEG(mxd_relief, outputjpg_relief, "PAGE_LAYOUT", 480, 640, 200, "False", "24-BIT_TRUE_COLOR", 100)
            if not os.path.exists(os.path.join(PSR_CAN_config.report_path, 'PSRmaps', OrderNumText)):
                os.mkdir(os.path.join(PSR_CAN_config.report_path, 'PSRmaps', OrderNumText))
            shutil.copy(outputjpg_relief, os.path.join(PSR_CAN_config.report_path, 'PSRmaps', OrderNumText))
            del mxd_relief
            del df_relief

            # part 2: the data driven pages
            page = 1

            page = int(arcpy.GetCount_management(gridlrshp).getOutput(0))  + page
            mxdMM_relief = arcpy.mapping.MapDocument(PSR_CAN_config.mxdMMfile_relief)

            dfMM_relief = arcpy.mapping.ListDataFrames(mxdMM_relief,"*")[0]
            dfMM_relief.spatialReference = spatialRef
            addBuffertoMxd("buffer_topo",dfMM_relief)
            addOrdergeomtoMxd("ordergeoNamePR", dfMM_relief)

            gridlayerMM = arcpy.mapping.ListLayers(mxdMM_relief,"Grid" ,dfMM_relief)[0]
            gridlayerMM.replaceDataSource(scratch, "FILEGDB_WORKSPACE","gridlr_relief")
            print("PageNumber")
            arcpy.CalculateAdjacentFields_cartography(gridlrshp, "PageNumber")
            mxdMM_relief.saveACopy(os.path.join(scratchfolder, "mxdMM_relief.mxd"))

            for item in img_selected:
                reliefLayer = arcpy.mapping.Layer(PSR_CAN_config.relieflyrfile)
                shutil.copy(os.path.join(PSR_CAN_config.path_shadedrelief,item),scratchfolder)   #make a local copy, will make it run faster
                reliefLayer.replaceDataSource(scratchfolder,"RASTER_WORKSPACE",item)
                reliefLayer.name = item[:-4]
                arcpy.mapping.AddLayer(dfMM_relief, reliefLayer, "BOTTOM")

            for i in range(1,int(arcpy.GetCount_management(gridlrshp).getOutput(0))+1):
                arcpy.SelectLayerByAttribute_management(gridlayerMM, "NEW_SELECTION", ' "PageNumber" =  ' + str(i))
                dfMM_relief.extent = gridlayerMM.getSelectedExtent(True)
                dfMM_relief.scale = dfMM_relief.scale * 1.1
                arcpy.SelectLayerByAttribute_management(gridlayerMM, "CLEAR_SELECTION")

                arcpy.mapping.ExportToJPEG(mxdMM_relief, outputjpg_relief[0:-4]+str(i)+".jpg", "PAGE_LAYOUT", 480, 640, 200, "False", "24-BIT_TRUE_COLOR", 100)
                if not os.path.exists(os.path.join(PSR_CAN_config.report_path, 'PSRmaps', OrderNumText)):
                    os.mkdir(os.path.join(PSR_CAN_config.report_path, 'PSRmaps', OrderNumText))
                shutil.copy(outputjpg_relief[0:-4]+str(i)+".jpg", os.path.join(PSR_CAN_config.report_path, 'PSRmaps', OrderNumText))
            del mxdMM_relief
            del dfMM_relief

        try:
            con = cx_Oracle.connect(PSR_CAN_config.connectionString)
            cur = con.cursor()
            if os.path.exists(os.path.join(PSR_CAN_config.report_path, 'PSRmaps', OrderNumText, OrderNumText+'_CA_TOPO.jpg')):
                query = cur.callproc('eris_psr.InsertMap', (OrderIDText, 'TOPO', OrderNumText+'_CA_TOPO.jpg', 1))
                if multipage_topo == True:
                    for i in range(1,page):
                        query = cur.callproc('eris_psr.InsertMap', (OrderIDText, 'TOPO', OrderNumText+'_CA_TOPO'+str(i)+'.jpg', i+1))

            else:
                print "No Topo map is available"

            if os.path.exists(os.path.join(PSR_CAN_config.report_path, 'PSRmaps', OrderNumText, OrderNumText+'_CA_RELIEF.jpg')):
                query = cur.callproc('eris_psr.InsertMap', (OrderIDText, 'RELIEF', OrderNumText+'_CA_RELIEF.jpg', 1))
                if multipage_relief == True:
                    for i in range(1,page):
                        query = cur.callproc('eris_psr.InsertMap', (OrderIDText, 'RELIEF', OrderNumText+'_CA_RELIEF'+str(i)+'.jpg', i+1))

            else:
                print "No Relief map is available"

        finally:
            cur.close()
            con.close()
    else:
         print "No Topo"
# 3 Wetland Map only, no attributes ------------------------------------------------------------------------------------------------------------------------
    if 'bufferDist_wetland' in locals():
        print "--- starting Wetland section " + time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
        bufferSHP_wetland = os.path.join(scratchfolder,"buffer_wetland.shp")
        arcpy.Buffer_analysis(orderGeometryPR, bufferSHP_wetland, bufferDist_wetland)

        mxd_wetland = arcpy.mapping.MapDocument(PSR_CAN_config.mxdfile_wetland)
        df_wetland = arcpy.mapping.ListDataFrames(mxd_wetland,"big")[0]
        df_wetland.spatialReference = spatialRef
        df_wetlandsmall = arcpy.mapping.ListDataFrames(mxd_wetland,"small")[0]
        df_wetlandsmall.spatialReference = spatialRef
        del df_wetlandsmall

        addBuffertoMxd("buffer_wetland",df_wetland)
        addOrdergeomtoMxd("ordergeoNamePR", df_wetland)

        masterLayer_wetland = arcpy.mapping.Layer(PSR_CAN_config.masterlyr_wetland)
        arcpy.SelectLayerByLocation_management(masterLayer_wetland,'intersect',bufferSHP_wetland)
        wetland = ''
        if(int((arcpy.GetCount_management(masterLayer_wetland).getOutput(0))) ==0):
            print "NO wetland data"
            masterLayer_wetland = None
            wetland = 'N'

        if multipage_wetland == False:
            mxd_wetland.saveACopy(os.path.join(scratchfolder, "mxd_wetland.mxd"))
            arcpy.mapping.ExportToJPEG(mxd_wetland, outputjpg_wetland, "PAGE_LAYOUT", 480, 640, 200, "False", "24-BIT_TRUE_COLOR", 100)
            if not os.path.exists(os.path.join(PSR_CAN_config.report_path, 'PSRmaps', OrderNumText)):
                os.mkdir(os.path.join(PSR_CAN_config.report_path, 'PSRmaps', OrderNumText))
            shutil.copy(outputjpg_wetland, os.path.join(PSR_CAN_config.report_path, 'PSRmaps', OrderNumText))
            del mxd_wetland
            del df_wetland

        else:    # multipage
            gridlr = "gridlr_wetland"   #gdb feature class doesn't work, could be a bug. So use .shp
            gridlrshp = os.path.join(scratch, gridlr)
            arcpy.GridIndexFeatures_cartography(gridlrshp, bufferSHP_wetland, "", "", "", gridsize, gridsize)  # note the tool takes featureclass name only, not the full path

            # part 1: the overview map
            # add grid layer
            gridLayer = arcpy.mapping.Layer(PSR_CAN_config.gridlyrfile)
            gridLayer.replaceDataSource(scratch,"FILEGDB_WORKSPACE","gridlr_wetland")
            arcpy.mapping.AddLayer(df_wetland,gridLayer,"Top")

            df_wetland.extent = gridLayer.getExtent()
            df_wetland.scale = df_wetland.scale * 1.1

            mxd_wetland.saveACopy(os.path.join(scratchfolder, "mxd_wetland.mxd"))
            arcpy.mapping.ExportToJPEG(mxd_wetland, outputjpg_wetland, "PAGE_LAYOUT", 480, 640, 200, "False", "24-BIT_TRUE_COLOR", 100)
            if not os.path.exists(os.path.join(PSR_CAN_config.report_path, 'PSRmaps', OrderNumText)):
                os.mkdir(os.path.join(PSR_CAN_config.report_path, 'PSRmaps', OrderNumText))
            shutil.copy(outputjpg_wetland, os.path.join(PSR_CAN_config.report_path, 'PSRmaps', OrderNumText))

            del mxd_wetland
            del df_wetland

            # part 2: the data driven pages
            page = 1

            page = int(arcpy.GetCount_management(gridlrshp).getOutput(0))  + page
            mxdMM_wetland = arcpy.mapping.MapDocument(PSR_CAN_config.mxdMMfile_wetland)

            dfMM_wetland = arcpy.mapping.ListDataFrames(mxdMM_wetland,"big")[0]
            dfMM_wetland.spatialReference = spatialRef
            addBuffertoMxd("buffer_wetland",dfMM_wetland)
            addOrdergeomtoMxd("ordergeoNamePR", dfMM_wetland)
            gridlayerMM = arcpy.mapping.ListLayers(mxdMM_wetland,"Grid" ,dfMM_wetland)[0]
            gridlayerMM.replaceDataSource(scratch, "FILEGDB_WORKSPACE","gridlr_wetland")
            arcpy.CalculateAdjacentFields_cartography(gridlrshp, "PageNumber")
            mxdMM_wetland.saveACopy(os.path.join(scratchfolder, "mxdMM_wetland.mxd"))

            for i in range(1,int(arcpy.GetCount_management(gridlrshp).getOutput(0))+1):
                arcpy.SelectLayerByAttribute_management(gridlayerMM, "NEW_SELECTION", ' "PageNumber" =  ' + str(i))
                dfMM_wetland.extent = gridlayerMM.getSelectedExtent(True)
                dfMM_wetland.scale = dfMM_wetland.scale * 1.1
                arcpy.SelectLayerByAttribute_management(gridlayerMM, "CLEAR_SELECTION")
                arcpy.RefreshTOC()

                arcpy.mapping.ExportToJPEG(mxdMM_wetland, outputjpg_wetland[0:-4]+str(i)+".jpg", "PAGE_LAYOUT", 480, 640, 200, "False", "24-BIT_TRUE_COLOR", 100)
                if not os.path.exists(os.path.join(PSR_CAN_config.report_path, 'PSRmaps', OrderNumText)):
                    os.mkdir(os.path.join(PSR_CAN_config.report_path, 'PSRmaps', OrderNumText))
                shutil.copy(outputjpg_wetland[0:-4]+str(i)+".jpg", os.path.join(PSR_CAN_config.report_path, 'PSRmaps', OrderNumText))
            del mxdMM_wetland
            del dfMM_wetland

        try:
            con = cx_Oracle.connect(PSR_CAN_config.connectionString)
            cur = con.cursor()

            query = cur.callproc('eris_psr.InsertMap', (OrderIDText, 'WETLAND', OrderNumText+'_CA_WETL.jpg', 1))
            if multipage_wetland == True:
                for i in range(1,page):
                    query = cur.callproc('eris_psr.InsertMap', (OrderIDText, 'WETLAND', OrderNumText+'_CA_WETL'+str(i)+'.jpg', i+1))

        finally:
            cur.close()
            con.close()
    else:
        print "no wetland"

# 4 GEOLOGY REPORT ------------------------------------------------------------------------------------------------------------------------
    if 'bufferDist_geol' in locals():
        # Bedrock GEOGLOGY
        print "--- starting geology " + time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
        bufferSHP_geol = os.path.join(scratchfolder,"buffer_geol.shp")
        arcpy.Buffer_analysis(orderGeometryPR, bufferSHP_geol, bufferDist_geol)
        bedrockclip_list=[]
        masterLayer_bedrock = arcpy.mapping.Layer(PSR_CAN_config.masterlyr_bedrock)
        arcpy.SelectLayerByLocation_management(masterLayer_bedrock,'intersect',bufferSHP_geol)
        if (int((arcpy.GetCount_management(masterLayer_bedrock).getOutput(0))) !=0):
            Bds_oids =[]
            mxd_geol = arcpy.mapping.MapDocument(PSR_CAN_config.mxdfile_geolB_prov)
            df_geol = arcpy.mapping.ListDataFrames(mxd_geol,"big")[0]
            df_geol.spatialReference = spatialRef
            df_geolsmall = arcpy.mapping.ListDataFrames(mxd_geol,"small")[0]
            df_geolsmall.spatialReference = spatialRef

            rows = arcpy.SearchCursor(masterLayer_bedrock)
            for row in rows:
                bedrockclip_list.append([row.Prov,str(row.dsoid),os.path.join(PSR_CAN_config.data_geol_bedrock_Prov_gdb,row.datasource),os.path.join(PSR_CAN_config.datalyr_folder,row.lyr)])
                # [prov, dsoid, data source, data layer]

            bedrockclips = []
            for bedrock_data in bedrockclip_list:
                bedrock_data = bedrock_data[2]
                bedrockclip = os.path.join(scratch,bedrock_data[-10:]+"_clip")
                arcpy.Clip_analysis(bedrock_data,bufferSHP_geol,bedrockclip)
                bedrockclips.append(bedrockclip)

            for bedrock_lyr in bedrockclip_list:
                bedrock_lyr=bedrock_lyr[3]
                bedrockLayer = arcpy.mapping.Layer(bedrock_lyr)
                arcpy.mapping.AddLayer(df_geol,bedrockLayer,"Top")
            addBuffertoMxd("buffer_geol",df_geol)
            addOrdergeomtoMxd("ordergeoNamePR", df_geol)
            if multipage_geology == False:
                # df.scale = 5000
                mxd_geol.saveACopy(os.path.join(scratchfolder, "mxd_geolB.mxd"))
                arcpy.mapping.ExportToJPEG(mxd_geol, outputjpg_geolB, "PAGE_LAYOUT", 480, 640, 200, "False", "24-BIT_TRUE_COLOR", 100)
                if not os.path.exists(os.path.join(PSR_CAN_config.report_path, 'PSRmaps', OrderNumText)):
                    os.mkdir(os.path.join(PSR_CAN_config.report_path, 'PSRmaps', OrderNumText))
                shutil.copy(outputjpg_geolB, os.path.join(PSR_CAN_config.report_path, 'PSRmaps', OrderNumText))
                del mxd_geol
                del df_geol

            else:    # multipage

                gridlr = "gridlr_geol"   # gdb feature class doesn't work, could be a bug. So use .shp
                gridlrshp = os.path.join(scratch, gridlr)
                arcpy.GridIndexFeatures_cartography(gridlrshp, bufferSHP_geol, "", "", "", gridsize, gridsize)  # note the tool takes featureclass name only, not the full path

                # part 1: the overview map
                # add grid layer
                gridLayer = arcpy.mapping.Layer(PSR_CAN_config.gridlyrfile)
                gridLayer.replaceDataSource(scratch,"FILEGDB_WORKSPACE","gridlr_geol")
                arcpy.mapping.AddLayer(df_geol,gridLayer,"Top")

                df_geol.extent = gridLayer.getExtent()
                df_geol.scale = df_geol.scale * 1.1

                mxd_geol.saveACopy(os.path.join(scratchfolder, "mxd_geolB.mxd"))
                arcpy.mapping.ExportToJPEG(mxd_geol, outputjpg_geolB, "PAGE_LAYOUT", 480, 640, 200, "False", "24-BIT_TRUE_COLOR", 100)
                if not os.path.exists(os.path.join(PSR_CAN_config.report_path, 'PSRmaps', OrderNumText)):
                    os.mkdir(os.path.join(PSR_CAN_config.report_path, 'PSRmaps', OrderNumText))
                shutil.copy(outputjpg_geolB, os.path.join(PSR_CAN_config.report_path, 'PSRmaps', OrderNumText))
                del mxd_geol
                del df_geol

                # part 2: the data driven pages
                page = 1
                page = int(arcpy.GetCount_management(gridlrshp).getOutput(0))  + page
                mxdMM_geol = arcpy.mapping.MapDocument(PSR_CAN_config.mxdMMfile_geolB_prov)
                dfMM_geol = arcpy.mapping.ListDataFrames(mxdMM_geol,"big")[0]
                dfMM_geol.spatialReference = spatialRef
                dfMM_geolsmall = arcpy.mapping.ListDataFrames(mxdMM_geol,"small")[0]
                dfMM_geolsmall.spatialReference = spatialRef
                bedrockLayer = arcpy.mapping.Layer(bedrock_lyr)
                # bedrockLayer.replaceDataSource(scratch,"FILEGDB_WORKSPACE","bedrock_clip")
                arcpy.mapping.AddLayer(dfMM_geol,bedrockLayer,"Top")
                addBuffertoMxd("buffer_geol",dfMM_geol)
                addOrdergeomtoMxd("ordergeoNamePR", dfMM_geol)

                gridlayerMM = arcpy.mapping.ListLayers(mxdMM_geol,"Grid" ,dfMM_geol)[0]
                gridlayerMM.replaceDataSource(scratch, "FILEGDB_WORKSPACE","gridlr_geol")
                arcpy.CalculateAdjacentFields_cartography(gridlrshp, "PageNumber")
                mxdMM_geol.saveACopy(os.path.join(scratchfolder, "mxdMM_geolB.mxd"))

                for i in range(1,int(arcpy.GetCount_management(gridlrshp).getOutput(0))+1):
                    arcpy.SelectLayerByAttribute_management(gridlayerMM, "NEW_SELECTION", ' "PageNumber" =  ' + str(i))
                    dfMM_geol.extent = gridlayerMM.getSelectedExtent(True)
                    dfMM_geol.scale = dfMM_geol.scale * 1.1
                    arcpy.SelectLayerByAttribute_management(gridlayerMM, "CLEAR_SELECTION")

                    titleTextE = arcpy.mapping.ListLayoutElements(mxdMM_geol, "TEXT_ELEMENT", "title")[0]
                    titleTextE.text = "Bedrock Geologic Types - Page " + str(i)
                    titleTextE.elementPositionX = 0.6303
                    arcpy.RefreshTOC()

                    arcpy.mapping.ExportToJPEG(mxdMM_geol, outputjpg_geolB[0:-4]+str(i)+".jpg", "PAGE_LAYOUT", 480, 640, 200, "False", "24-BIT_TRUE_COLOR", 100)
                    if not os.path.exists(os.path.join(PSR_CAN_config.report_path, 'PSRmaps', OrderNumText)):
                        os.mkdir(os.path.join(PSR_CAN_config.report_path, 'PSRmaps', OrderNumText))
                    shutil.copy(outputjpg_geolB[0:-4]+str(i)+".jpg", os.path.join(PSR_CAN_config.report_path, 'PSRmaps', OrderNumText))
                del mxdMM_geol
                del dfMM_geol
            geolB = ''
            bedrockclip_merge = arcpy.Merge_management(bedrockclips,os.path.join(scratch,"becdrockclip_merge"))
            if (int(arcpy.GetCount_management(bedrockclip_merge).getOutput(0))== 0):
                # no geology polygon selected...., need to send in map only
                print 'No geology polygon is selected....'
                geolB = 'N'
                try:
                    con = cx_Oracle.connect(PSR_CAN_config.connectionString)
                    # con = cx_Oracle.connect('eris/eris@GMTEST.glaciermedia.inc')
                    cur = con.cursor()

                    query = cur.callproc('eris_psr.InsertMap', (OrderIDText, 'GEOL_B', OrderNumText+'_CA_Bedrock_GEOL.jpg', 1))          #note type 'SOIL' or 'GEOL' is used internally

                finally:
                    cur.close()
                    con.close()
            else:
                try:
                    con = cx_Oracle.connect(PSR_CAN_config.connectionString)
                    cur = con.cursor()
                    geologyB_IDs = []
                    reportingList = []
                    in_rows = arcpy.SearchCursor(bedrockclip_merge)
                    for in_row in in_rows:
                        for blist in bedrockclip_list:
                            bedrock_fields = eval("PSR_CAN_config."+blist[0]+"_bedrock")
                            tempID = str(in_row.getValue(bedrock_fields.keys()[0]))
                            if tempID !="None" and tempID not in reportingList:
                                reportingList.append(tempID)
                                erisid = erisid + 1
                                geologyB_IDs.append([str(in_row.getValue(bedrock_fields.keys()[0])),erisid])
                                cur.callproc('eris_psr.InsertOrderDetail', (OrderIDText, erisid,blist[1]))
                                query = cur.callproc('eris_psr.InsertFlexRep', (OrderIDText, erisid, blist[1], 2, 'S1', 1, bedrock_fields.values()[0]+" "+ str(in_row.getValue(bedrock_fields.keys()[0])), ''))
                                print query
                                i =2
                                for key in bedrock_fields.keys()[1:]:
                                    query = cur.callproc('eris_psr.InsertFlexRep', (OrderIDText, erisid, blist[1], 2, 'N',i, bedrock_fields[key]+": ", str(in_row.getValue(key).encode("utf-8"))))
                                    i = i +1
                                    print query

                    del in_row
                    del in_rows
                    query = cur.callproc('eris_psr.InsertMap', (OrderIDText, 'GEOL_B', OrderNumText+'_CA_Bedrock_GEOL.jpg', 1))          #note type 'SOIL' or 'GEOL' is used internally
                    if multipage_geology == True:
                        for i in range(1,page):
                            query = cur.callproc('eris_psr.InsertMap', (OrderIDText, 'GEOL_B', OrderNumText+'_CA_Bedrock_GEOL'+str(i)+'.jpg', i+1))
                finally:
                    cur.close()
                    con.close()

        else: # no bedrock provincal data available
            geol_clip =os.path.join(scratch,'geologyB')   # better keep in file geodatabase due to content length in certain columns
            bedrock_data = PSR_CAN_config.data_geol_bedrock
            bedrock_lyr = PSR_CAN_config.datalyr_geologyB
            arcpy.Clip_analysis(bedrock_data, bufferSHP_geol, geol_clip)
            bedrockclip_list.append(['CA','12913',bedrock_data,bedrock_lyr])
                # [prov, dsoid, data source, data layer]

            mxd_geol = arcpy.mapping.MapDocument(PSR_CAN_config.mxdfile_geolB)
            df_geol = arcpy.mapping.ListDataFrames(mxd_geol,"big")[0]
            df_geol.spatialReference = spatialRef
            df_geolsmall = arcpy.mapping.ListDataFrames(mxd_geol,"small")[0]
            df_geolsmall.spatialReference = spatialRef

            addBuffertoMxd("buffer_geol",df_geol)
            addOrdergeomtoMxd("ordergeoNamePR", df_geol)

            if multipage_geology == False:
                # df.scale = 5000
                mxd_geol.saveACopy(os.path.join(scratchfolder, "mxd_geolB.mxd"))
                arcpy.mapping.ExportToJPEG(mxd_geol, outputjpg_geolB, "PAGE_LAYOUT", 480, 640, 200, "False", "24-BIT_TRUE_COLOR", 100)
                if not os.path.exists(os.path.join(PSR_CAN_config.report_path, 'PSRmaps', OrderNumText)):
                    os.mkdir(os.path.join(PSR_CAN_config.report_path, 'PSRmaps', OrderNumText))
                shutil.copy(outputjpg_geolB, os.path.join(PSR_CAN_config.report_path, 'PSRmaps', OrderNumText))
                del mxd_geol
                del df_geol

            else:    # multipage

                gridlr = "gridlr_geol"   # gdb feature class doesn't work, could be a bug. So use .shp
                gridlrshp = os.path.join(scratch, gridlr)
                arcpy.GridIndexFeatures_cartography(gridlrshp, bufferSHP_geol, "", "", "", gridsize, gridsize)  # note the tool takes featureclass name only, not the full path

                # part 1: the overview map
                # add grid layer
                gridLayer = arcpy.mapping.Layer(PSR_CAN_config.gridlyrfile)
                gridLayer.replaceDataSource(scratch,"FILEGDB_WORKSPACE","gridlr_geol")
                arcpy.mapping.AddLayer(df_geol,gridLayer,"Top")

                df_geol.extent = gridLayer.getExtent()
                df_geol.scale = df_geol.scale * 1.1

                mxd_geol.saveACopy(os.path.join(scratchfolder, "mxd_geolB.mxd"))
                arcpy.mapping.ExportToJPEG(mxd_geol, outputjpg_geolB, "PAGE_LAYOUT", 480, 640, 200, "False", "24-BIT_TRUE_COLOR", 100)
                if not os.path.exists(os.path.join(PSR_CAN_config.report_path, 'PSRmaps', OrderNumText)):
                    os.mkdir(os.path.join(PSR_CAN_config.report_path, 'PSRmaps', OrderNumText))
                shutil.copy(outputjpg_geolB, os.path.join(PSR_CAN_config.report_path, 'PSRmaps', OrderNumText))

                del mxd_geol
                del df_geol

                # part 2: the data driven pages
                page = 1

                page = int(arcpy.GetCount_management(gridlrshp).getOutput(0))  + page
                mxdMM_geol = arcpy.mapping.MapDocument(PSR_CAN_config.mxdMMfile_geolB)

                dfMM_geol = arcpy.mapping.ListDataFrames(mxdMM_geol,"big")[0]
                dfMM_geol.spatialReference = spatialRef
                dfMM_geolsmall = arcpy.mapping.ListDataFrames(mxdMM_geol,"small")[0]
                dfMM_geolsmall.spatialReference = spatialRef
                addBuffertoMxd("buffer_geol",dfMM_geol)
                addOrdergeomtoMxd("ordergeoNamePR", dfMM_geol)

                gridlayerMM = arcpy.mapping.ListLayers(mxdMM_geol,"Grid" ,dfMM_geol)[0]
                gridlayerMM.replaceDataSource(scratch, "FILEGDB_WORKSPACE","gridlr_geol")
                arcpy.CalculateAdjacentFields_cartography(gridlrshp, "PageNumber")
                mxdMM_geol.saveACopy(os.path.join(scratchfolder, "mxdMM_geolB.mxd"))

                for i in range(1,int(arcpy.GetCount_management(gridlrshp).getOutput(0))+1):
                    arcpy.SelectLayerByAttribute_management(gridlayerMM, "NEW_SELECTION", ' "PageNumber" =  ' + str(i))
                    dfMM_geol.extent = gridlayerMM.getSelectedExtent(True)
                    dfMM_geol.scale = dfMM_geol.scale * 1.1
                    arcpy.SelectLayerByAttribute_management(gridlayerMM, "CLEAR_SELECTION")

                    titleTextE = arcpy.mapping.ListLayoutElements(mxdMM_geol, "TEXT_ELEMENT", "title")[0]
                    titleTextE.text = "Bedrock Geologic Types - Page " + str(i)
                    titleTextE.elementPositionX = 0.6303
                    arcpy.RefreshTOC()

                    arcpy.mapping.ExportToJPEG(mxdMM_geol, outputjpg_geolB[0:-4]+str(i)+".jpg", "PAGE_LAYOUT", 480, 640, 200, "False", "24-BIT_TRUE_COLOR", 100)
                    if not os.path.exists(os.path.join(PSR_CAN_config.report_path, 'PSRmaps', OrderNumText)):
                        os.mkdir(os.path.join(PSR_CAN_config.report_path, 'PSRmaps', OrderNumText))
                    shutil.copy(outputjpg_geolB[0:-4]+str(i)+".jpg", os.path.join(PSR_CAN_config.report_path, 'PSRmaps', OrderNumText))
                del mxdMM_geol
                del dfMM_geol
            geolB = ''
            if (int(arcpy.GetCount_management(geol_clip).getOutput(0))== 0):
                # no geology polygon selected...., need to send in map only
                print 'No geology polygon is selected....'
                geolB = 'N'
                try:
                    con = cx_Oracle.connect(PSR_CAN_config.connectionString)
                    # con = cx_Oracle.connect('eris/eris@GMTEST.glaciermedia.inc')
                    cur = con.cursor()

                    query = cur.callproc('eris_psr.InsertMap', (OrderIDText, 'GEOL_B', OrderNumText+'_CA_Bedrock_GEOL.jpg', 1))          #note type 'SOIL' or 'GEOL' is used internally

                finally:
                    cur.close()
                    con.close()
            else:
                try:
                    con = cx_Oracle.connect(PSR_CAN_config.connectionString)
                    cur = con.cursor()
                    geologyB_IDs = []
                    reportingList = []
                    in_rows = arcpy.SearchCursor(geol_clip)
                    for in_row in in_rows:
                        tempID = str(in_row.UNIT)
                        if tempID !="None" and tempID not in reportingList:
                            reportingList.append(tempID)
                            erisid = erisid + 1
                            geologyB_IDs.append([in_row.UNIT,erisid])
                            cur.callproc('eris_psr.InsertOrderDetail', (OrderIDText, erisid,PSR_CAN_config.BGEC))
                            query = cur.callproc('eris_psr.InsertFlexRep', (OrderIDText, erisid, PSR_CAN_config.BGEC, 2, 'S1', 1, 'Unit ID ' + str(in_row.UNIT), ''))
                            query = cur.callproc('eris_psr.InsertFlexRep', (OrderIDText, erisid, PSR_CAN_config.BGEC, 2, 'N', 2, 'Primary Rock type : ', in_row.RXTP))
                            query = cur.callproc('eris_psr.InsertFlexRep', (OrderIDText, erisid, PSR_CAN_config.BGEC, 2, 'N', 3, 'Secondary Rock Type: ', in_row.SUBRXTP))
                            query = cur.callproc('eris_psr.InsertFlexRep', (OrderIDText, erisid, PSR_CAN_config.BGEC, 2, 'N', 4, 'Era: ', in_row.ERA))
                            query = cur.callproc('eris_psr.InsertFlexRep', (OrderIDText, erisid, PSR_CAN_config.BGEC, 2, 'N', 5, 'Period: ', in_row.PERIOD))
                            query = cur.callproc('eris_psr.InsertFlexRep', (OrderIDText, erisid, PSR_CAN_config.BGEC, 2, 'N', 6, 'Epoch: ', in_row.EPOCH))
                            query = cur.callproc('eris_psr.InsertFlexRep', (OrderIDText, erisid, PSR_CAN_config.BGEC, 2, 'N', 7, 'Geological Province: ', in_row.GEOLPROV))

                    del in_row
                    del in_rows

                    query = cur.callproc('eris_psr.InsertMap', (OrderIDText, 'GEOL_B', OrderNumText+'_CA_Bedrock_GEOL.jpg', 1))          #note type 'SOIL' or 'GEOL' is used internally
                    if multipage_geology == True:
                        for i in range(1,page):
                            query = cur.callproc('eris_psr.InsertMap', (OrderIDText, 'GEOL_B', OrderNumText+'_CA_Bedrock_GEOL'+str(i)+'.jpg', i+1))
                finally:
                    cur.close()
                    con.close()

        # Surficial Geology
        masterLayer_surficial = arcpy.mapping.Layer(PSR_CAN_config.masterlyr_surficial)
        arcpy.SelectLayerByLocation_management(masterLayer_surficial,'intersect',bufferSHP_geol)
        surficialclip_list = []
        if (int((arcpy.GetCount_management(masterLayer_surficial).getOutput(0))) !=0):
            mxd_geol = arcpy.mapping.MapDocument(PSR_CAN_config.mxdfile_geolS_prov)
            df_geol = arcpy.mapping.ListDataFrames(mxd_geol,"big")[0]
            df_geol.spatialReference = spatialRef
            df_geolsmall = arcpy.mapping.ListDataFrames(mxd_geol,"small")[0]
            df_geolsmall.spatialReference = spatialRef

            surficialclip = os.path.join(scratch,"surficial_clip")

            rows = arcpy.SearchCursor(masterLayer_surficial)
            for row in rows:
                surficialclip_list.append([row.Prov,row.dsoid, os.path.join(PSR_CAN_config.data_geol_bedrock_Prov_gdb,row.datasource), os.path.join(PSR_CAN_config.datalyr_folder,row.lyr)])

            surficialclips = []
            for surficial_data in surficialclip_list:
                surficial_data = surficial_data[2]
                surficialclip = os.path.join(scratch,surficial_data[-12:]+"_clip")
                arcpy.Clip_analysis(surficial_data,bufferSHP_geol,surficialclip,"0.000001 DecimalDegrees")
                surficialclips.append(surficialclip)

            for surficial_lyr in surficialclip_list:
                surficial_lyr = surficial_lyr[3]
                surficialLayer = arcpy.mapping.Layer(surficial_lyr)
                arcpy.mapping.AddLayer(df_geol,surficialLayer,"Top")

            addBuffertoMxd("buffer_geol",df_geol)
            addOrdergeomtoMxd("ordergeoNamePR", df_geol)
            # df_geol.panToExtent(newLayerBuffer2.getExtent())
            if multipage_geology == False:
                # df.scale = 5000
                mxd_geol.saveACopy(os.path.join(scratchfolder, "mxd_geolS.mxd"))
                arcpy.mapping.ExportToJPEG(mxd_geol, outputjpg_geolS, "PAGE_LAYOUT", 480, 640, 200, "False", "24-BIT_TRUE_COLOR", 100)
                if not os.path.exists(os.path.join(PSR_CAN_config.report_path, 'PSRmaps', OrderNumText)):
                    os.mkdir(os.path.join(PSR_CAN_config.report_path, 'PSRmaps', OrderNumText))
                shutil.copy(outputjpg_geolS, os.path.join(PSR_CAN_config.report_path, 'PSRmaps', OrderNumText))
                del mxd_geol
                del df_geol

            else:    # multipage

                gridlr = "gridlr_geol"   # gdb feature class doesn't work, could be a bug. So use .shp
                gridlrshp = os.path.join(scratch, gridlr)
                arcpy.GridIndexFeatures_cartography(gridlrshp, bufferSHP_geol, "", "", "", gridsize, gridsize)  # note the tool takes featureclass name only, not the full path

                # part 1: the overview map
                # add grid layer
                gridLayer = arcpy.mapping.Layer(PSR_CAN_config.gridlyrfile)
                gridLayer.replaceDataSource(scratch,"FILEGDB_WORKSPACE","gridlr_geol")
                arcpy.mapping.AddLayer(df_geol,gridLayer,"Top")

                df_geol.extent = gridLayer.getExtent()
                df_geol.scale = df_geol.scale * 1.1

                mxd_geol.saveACopy(os.path.join(scratchfolder, "mxd_geolS.mxd"))
                arcpy.mapping.ExportToJPEG(mxd_geol, outputjpg_geolS, "PAGE_LAYOUT", 480, 640, 200, "False", "24-BIT_TRUE_COLOR", 100)
                if not os.path.exists(os.path.join(PSR_CAN_config.report_path, 'PSRmaps', OrderNumText)):
                    os.mkdir(os.path.join(PSR_CAN_config.report_path, 'PSRmaps', OrderNumText))
                shutil.copy(outputjpg_geolS, os.path.join(PSR_CAN_config.report_path, 'PSRmaps', OrderNumText))
                del mxd_geol
                del df_geol

                # part 2: the data driven pages
                page = 1
                page = int(arcpy.GetCount_management(gridlrshp).getOutput(0))  + page
                mxdMM_geol = arcpy.mapping.MapDocument(PSR_CAN_config.mxdMMfile_geolS_prov)

                dfMM_geol = arcpy.mapping.ListDataFrames(mxdMM_geol,"big")[0]
                dfMM_geol.spatialReference = spatialRef
                dfMM_geolsmall = arcpy.mapping.ListDataFrames(mxdMM_geol,"small")[0]
                dfMM_geolsmall.spatialReference = spatialRef
                surficialLayer = arcpy.mapping.Layer(surficial_lyr)
                arcpy.mapping.AddLayer(dfMM_geol,surficialLayer,"Top")
                addBuffertoMxd("buffer_geol",dfMM_geol)
                addOrdergeomtoMxd("ordergeoNamePR", dfMM_geol)

                gridlayerMM = arcpy.mapping.ListLayers(mxdMM_geol,"Grid" ,dfMM_geol)[0]
                gridlayerMM.replaceDataSource(scratch, "FILEGDB_WORKSPACE","gridlr_geol")
                arcpy.CalculateAdjacentFields_cartography(gridlrshp, "PageNumber")
                mxdMM_geol.saveACopy(os.path.join(scratchfolder, "mxdMM_geolS.mxd"))

                for i in range(1,int(arcpy.GetCount_management(gridlrshp).getOutput(0))+1):
                    arcpy.SelectLayerByAttribute_management(gridlayerMM, "NEW_SELECTION", ' "PageNumber" =  ' + str(i))
                    dfMM_geol.extent = gridlayerMM.getSelectedExtent(True)
                    dfMM_geol.scale = dfMM_geol.scale * 1.1
                    arcpy.SelectLayerByAttribute_management(gridlayerMM, "CLEAR_SELECTION")

                    titleTextE = arcpy.mapping.ListLayoutElements(mxdMM_geol, "TEXT_ELEMENT", "title")[0]
                    titleTextE.text = "Surficial Geology - Page " + str(i)
                    titleTextE.elementPositionX = 0.6303
                    arcpy.RefreshTOC()

                    arcpy.mapping.ExportToJPEG(mxdMM_geol, outputjpg_geolS[0:-4]+str(i)+".jpg", "PAGE_LAYOUT", 480, 640, 200, "False", "24-BIT_TRUE_COLOR", 100)
                    if not os.path.exists(os.path.join(PSR_CAN_config.report_path, 'PSRmaps', OrderNumText)):
                        os.mkdir(os.path.join(PSR_CAN_config.report_path, 'PSRmaps', OrderNumText))
                    shutil.copy(outputjpg_geolS[0:-4]+str(i)+".jpg", os.path.join(PSR_CAN_config.report_path, 'PSRmaps', OrderNumText))
                del mxdMM_geol
                del dfMM_geol
            geolS = ''
            surficialclip_merge = arcpy.Merge_management(surficialclips,os.path.join(scratch,"surficialclip_merge"))
            if (int(arcpy.GetCount_management(surficialclip_merge).getOutput(0))== 0):
                # no geology polygon selected...., need to send in map only
                print 'No geology polygon is selected....'
                geolS = 'N'
                try:
                    con = cx_Oracle.connect(PSR_CAN_config.connectionString)
                    # con = cx_Oracle.connect('eris/eris@GMTEST.glaciermedia.inc')
                    cur = con.cursor()
                    query = cur.callproc('eris_psr.InsertMap', (OrderIDText, 'GEOL_S', OrderNumText+'_CA_Bedrock_GEOL.jpg', 1))          # note type 'SOIL' or 'GEOL' is used internally

                finally:
                    cur.close()
                    con.close()
            else:
                try:
                    con = cx_Oracle.connect(PSR_CAN_config.connectionString)
                    cur = con.cursor()
                    geologyS_IDs = []
                    reportingList =[]
                    in_rows = arcpy.SearchCursor(surficialclip_merge)
                    for in_row in in_rows:
                        for slist in surficialclip_list:
                            surficial_fields = eval("PSR_CAN_config."+slist[0]+"_surficial")
                            tempID = str(in_row.getValue(surficial_fields.keys()[0]))
                            if tempID !="None" and tempID not in reportingList:
                                reportingList.append(tempID)
                                erisid = erisid + 1
                                geologyS_IDs.append([str(in_row.getValue(surficial_fields.keys()[0])),erisid])
                                cur.callproc('eris_psr.InsertOrderDetail', (OrderIDText, erisid,slist[1]))
                                query = cur.callproc('eris_psr.InsertFlexRep', (OrderIDText, erisid, slist[1], 2, 'S1', 1, surficial_fields.values()[0]+" "+ str(in_row.getValue(surficial_fields.keys()[0])), ''))
                                print query
                                i =2
                                for key in surficial_fields.keys()[1:]:
                                    query = cur.callproc('eris_psr.InsertFlexRep', (OrderIDText, erisid, slist[1], 2, 'N',i, surficial_fields[key]+": ", str(in_row.getValue(key))))
                                    i = i +1
                                    print query

                    del in_row
                    del in_rows
                    query = cur.callproc('eris_psr.InsertMap', (OrderIDText, 'GEOL_S', OrderNumText+'_CA_Surficial_GEOL.jpg', 1))          #note type 'SOIL' or 'GEOL' is used internally
                    if multipage_geology == True:
                        for i in range(1,page):
                            query = cur.callproc('eris_psr.InsertMap', (OrderIDText, 'GEOL_S', OrderNumText+'_CA_Surficial_GEOL'+str(i)+'.jpg', i+1))
                finally:
                    cur.close()
                    con.close()

        else: # no Surficial provincal data available
            geolS_clip =os.path.join(scratch,'geologyS')   # better keep in file geodatabase due to content length in certain columns
            surficial_data = PSR_CAN_config.data_geol_surficial
            surficial_lyr = PSR_CAN_config.datalyr_geologyS
            arcpy.Clip_analysis(surficial_data, bufferSHP_geol, geolS_clip)
            surficialclip_list.append(['CA','12912', surficial_data, surficial_lyr])

            mxd_geol = arcpy.mapping.MapDocument(PSR_CAN_config.mxdfile_geolS)
            df_geol = arcpy.mapping.ListDataFrames(mxd_geol,"big")[0]
            df_geol.spatialReference = spatialRef
            df_geolsmall = arcpy.mapping.ListDataFrames(mxd_geol,"small")[0]
            df_geolsmall.spatialReference = spatialRef

            addBuffertoMxd("buffer_geol",df_geol)
            addOrdergeomtoMxd("ordergeoNamePR", df_geol)

            if multipage_geology == False:
                #d f.scale = 5000
                mxd_geol.saveACopy(os.path.join(scratchfolder, "mxd_geolS.mxd"))
                arcpy.mapping.ExportToJPEG(mxd_geol, outputjpg_geolS, "PAGE_LAYOUT", 480, 640, 200, "False", "24-BIT_TRUE_COLOR", 100)
                if not os.path.exists(os.path.join(PSR_CAN_config.report_path, 'PSRmaps', OrderNumText)):
                    os.mkdir(os.path.join(PSR_CAN_config.report_path, 'PSRmaps', OrderNumText))
                shutil.copy(outputjpg_geolS, os.path.join(PSR_CAN_config.report_path, 'PSRmaps', OrderNumText))
                del mxd_geol
                del df_geol

            else:    # multipage

                gridlr = "gridlr_geol"   # gdb feature class doesn't work, could be a bug. So use .shp
                gridlrshp = os.path.join(scratch, gridlr)
                arcpy.GridIndexFeatures_cartography(gridlrshp, bufferSHP_geol, "", "", "", gridsize, gridsize)  # note the tool takes featureclass name only, not the full path

                # part 1: the overview map
                # add grid layer
                gridLayer = arcpy.mapping.Layer(PSR_CAN_config.gridlyrfile)
                gridLayer.replaceDataSource(scratch,"FILEGDB_WORKSPACE","gridlr_geol")
                arcpy.mapping.AddLayer(df_geol,gridLayer,"Top")

                df_geol.extent = gridLayer.getExtent()
                df_geol.scale = df_geol.scale * 1.1

                mxd_geol.saveACopy(os.path.join(scratchfolder, "mxd_geolS.mxd"))
                arcpy.mapping.ExportToJPEG(mxd_geol, outputjpg_geolS, "PAGE_LAYOUT", 480, 640, 200, "False", "24-BIT_TRUE_COLOR", 100)
                if not os.path.exists(os.path.join(PSR_CAN_config.report_path, 'PSRmaps', OrderNumText)):
                    os.mkdir(os.path.join(PSR_CAN_config.report_path, 'PSRmaps', OrderNumText))
                shutil.copy(outputjpg_geolS, os.path.join(PSR_CAN_config.report_path, 'PSRmaps', OrderNumText))

                del mxd_geol
                del df_geol

                # part 2: the data driven pages
                page = 1

                page = int(arcpy.GetCount_management(gridlrshp).getOutput(0))  + page
                mxdMM_geol = arcpy.mapping.MapDocument(PSR_CAN_config.mxdMMfile_geolS)

                dfMM_geol = arcpy.mapping.ListDataFrames(mxdMM_geol,"big")[0]
                dfMM_geol.spatialReference = spatialRef
                dfMM_geolsmall = arcpy.mapping.ListDataFrames(mxdMM_geol,"small")[0]
                dfMM_geolsmall.spatialReference = spatialRef
                addBuffertoMxd("buffer_geol",dfMM_geol)
                addOrdergeomtoMxd("ordergeoNamePR", dfMM_geol)

                gridlayerMM = arcpy.mapping.ListLayers(mxdMM_geol,"Grid" ,dfMM_geol)[0]
                gridlayerMM.replaceDataSource(scratch, "FILEGDB_WORKSPACE","gridlr_geol")
                arcpy.CalculateAdjacentFields_cartography(gridlrshp, "PageNumber")
                mxdMM_geol.saveACopy(os.path.join(scratchfolder, "mxdMM_geolS.mxd"))

                for i in range(1,int(arcpy.GetCount_management(gridlrshp).getOutput(0))+1):
                    arcpy.SelectLayerByAttribute_management(gridlayerMM, "NEW_SELECTION", ' "PageNumber" =  ' + str(i))
                    dfMM_geol.extent = gridlayerMM.getSelectedExtent(True)
                    dfMM_geol.scale = dfMM_geol.scale * 1.1
                    arcpy.SelectLayerByAttribute_management(gridlayerMM, "CLEAR_SELECTION")

                    titleTextE = arcpy.mapping.ListLayoutElements(mxdMM_geol, "TEXT_ELEMENT", "title")[0]
                    titleTextE.text = "Surficial Geologic Types - Page " + str(i)
                    titleTextE.elementPositionX = 0.6303
                    arcpy.RefreshTOC()

                    arcpy.mapping.ExportToJPEG(mxdMM_geol, outputjpg_geolS[0:-4]+str(i)+".jpg", "PAGE_LAYOUT", 480, 640, 200, "False", "24-BIT_TRUE_COLOR", 100)
                    if not os.path.exists(os.path.join(PSR_CAN_config.report_path, 'PSRmaps', OrderNumText)):
                        os.mkdir(os.path.join(PSR_CAN_config.report_path, 'PSRmaps', OrderNumText))
                    shutil.copy(outputjpg_geolS[0:-4]+str(i)+".jpg", os.path.join(PSR_CAN_config.report_path, 'PSRmaps', OrderNumText))
                del mxdMM_geol
                del dfMM_geol
            geolS = ''
            if (int(arcpy.GetCount_management(geolS_clip).getOutput(0))== 0):
                # no geology polygon selected...., need to send in map only
                print 'No geology polygon is selected....'
                geolS = 'N'
                try:
                    con = cx_Oracle.connect(PSR_CAN_config.connectionString)
                    # con = cx_Oracle.connect('eris/eris@GMTEST.glaciermedia.inc')
                    cur = con.cursor()
                    query = cur.callproc('eris_psr.InsertMap', (OrderIDText, 'GEOL_S', OrderNumText+'_CA_Surficial_GEOL.jpg', 1))          # note type 'SOIL' or 'GEOL' is used internally

                finally:
                    cur.close()
                    con.close()

            else:
                try:
                    con = cx_Oracle.connect(PSR_CAN_config.connectionString)
                    cur = con.cursor()
                    geologyS_IDs = []
                    reportingList=[]
                    in_rows = arcpy.SearchCursor(geolS_clip)
                    for in_row in in_rows:
                        tempID = str(in_row.ULABEL1)
                        if tempID !="None" and tempID not in reportingList:
                            reportingList.append(tempID)
                            print "Unit type is: " + in_row.UTYPE1
                            print "Unit label is: " + in_row.ULABEL1
                            print "Hydro is: " + in_row.HYDRO_INT
                            erisid = erisid + 1
                            geologyS_IDs.append([in_row.ULABEL1,erisid])
                            cur.callproc('eris_psr.InsertOrderDetail', (OrderIDText, erisid,PSR_CAN_config.SGEC))
                            query = cur.callproc('eris_psr.InsertFlexRep', (OrderIDText, erisid, PSR_CAN_config.SGEC, 2, 'S1', 1, 'Unit ID ' + str(in_row.ULABEL1), ''))
                            query = cur.callproc('eris_psr.InsertFlexRep', (OrderIDText, erisid, PSR_CAN_config.SGEC, 2, 'N', 2, 'Unit Name: ', in_row.UTYPE1))
                            query = cur.callproc('eris_psr.InsertFlexRep', (OrderIDText, erisid, PSR_CAN_config.SGEC, 2, 'N', 3, 'Hydro: ', in_row.HYDRO_INT))
                    del in_row
                    del in_rows

                    query = cur.callproc('eris_psr.InsertMap', (OrderIDText, 'GEOL_S', OrderNumText+'_CA_Surficial_GEOL.jpg', 1))          #note type 'SOIL' or 'GEOL' is used internally
                    if multipage_geology == True:
                        for i in range(1,page):
                            query = cur.callproc('eris_psr.InsertMap', (OrderIDText, 'GEOL_S', OrderNumText+'CA_Surficial_GEOL'+str(i)+'.jpg', i+1))

                finally:
                    cur.close()
                    con.close()

# 5 SOIL ------------------------------------------------------------------------------------------------------------------------
    if 'bufferDist_soil' in locals():
        print "--- starting Soil section " + time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())

        bufferSHP_soil = os.path.join(scratchfolder,"buffer_soil.shp")
        arcpy.Buffer_analysis(orderGeometryPR, bufferSHP_soil, bufferDist_soil)
        fc_soils = os.path.join(scratchfolder,"soils.shp")
        fc_soils_PR = os.path.join(scratchfolder, "soilsPR.shp")

        masterLayer_soil = arcpy.mapping.Layer(PSR_CAN_config.masterlyr_soil)
        arcpy.SelectLayerByLocation_management(masterLayer_soil,'intersect',bufferSHP_soil)
        soildatalist =[]
        soil_pkeys = []

        if(int((arcpy.GetCount_management(masterLayer_soil).getOutput(0))) !=0):
            rows = arcpy.SearchCursor(masterLayer_soil)
            for row in rows:
                soildatalist.append([os.path.join(PSR_CAN_config.data_soilGDB,row.filename),row.DS_OID])
                soil_pkeys.append(row.PKey)

            tempclip = []
            for i in range(len(soildatalist)):
                fc_soil_temp = os.path.join(scratchfolder,"soilC_"+str(i)+".shp")
                arcpy.Clip_analysis(soildatalist[i][0],bufferSHP_soil,fc_soil_temp)
                arcpy.AddField_management(fc_soil_temp, "DS_OID", "TEXT", "", "", "", "", "NON_NULLABLE", "REQUIRED", "")
                rows = arcpy.UpdateCursor(fc_soil_temp)
                for row in rows:
                    row.DS_OID = soildatalist[i][1]
                    rows.updateRow(row)
                del rows
                tempclip.append(fc_soil_temp)
            arcpy.Merge_management(tempclip,fc_soils)
        else:
            soildatalist.append([PSR_CAN_config.data_soilScape,'12886'])
            arcpy.Clip_analysis(PSR_CAN_config.data_soilScape,bufferSHP_soil,fc_soils)
            arcpy.AddField_management(fc_soils, "DS_OID", "TEXT", "", "", "", "", "NON_NULLABLE", "REQUIRED", "")
            rows = arcpy.UpdateCursor(fc_soils)
            for row in rows:
                    row.DS_OID = '12886'
                    rows.updateRow(row)
            del rows
            soil_pkeys.append('POLY_ID')
        print 'soil map key and dsoid: '
        print soil_pkeys
        soil = ''
        arcpy.MakeFeatureLayer_management(fc_soils,'soillayer')
        if (int(arcpy.GetCount_management('soillayer').getOutput(0)) == 0):   # no soil polygons selected
            print 'no polygons selected'
            soil = 'N'
        else:

            arcpy.Project_management(fc_soils, fc_soils_PR, out_coordinate_system)
            ## create the map
            point = arcpy.Point()
            array = arcpy.Array()
            featureList = []
            width = arcpy.Describe(bufferSHP_soil).extent.width/2
            height = arcpy.Describe(bufferSHP_soil).extent.height/2
            if (width > 662 or height > 662):
                if (width/height > 1):
                   # buffer has a wider shape
                   width = width * 1.1
                   height = width
                else:
                    # buffer has a vertically elonged shape
                    height = height * 1.1
                    width = height
            else:
                width = 662*1.1
                height = 662*1.1
            width = width + 6400     # add 2 miles to each side, for multipage soil
            height = height + 6400   # add 2 miles to each side, for multipage soil
            xCentroid = (arcpy.Describe(bufferSHP_soil).extent.XMax + arcpy.Describe(bufferSHP_soil).extent.XMin)/2
            yCentroid = (arcpy.Describe(bufferSHP_soil).extent.YMax + arcpy.Describe(bufferSHP_soil).extent.YMin)/2
            point.X = xCentroid-width
            point.Y = yCentroid+height
            array.add(point)
            point.X = xCentroid+width
            point.Y = yCentroid+height
            array.add(point)
            point.X = xCentroid+width
            point.Y = yCentroid-height
            array.add(point)
            point.X = xCentroid-width
            point.Y = yCentroid-height
            array.add(point)
            point.X = xCentroid-width
            point.Y = yCentroid+height
            array.add(point)
            feat = arcpy.Polygon(array,spatialRef)
            array.removeAll()
            featureList.append(feat)
            clipFrame = os.path.join(scratchfolder, "clipFrame.shp")
            arcpy.CopyFeatures_management(featureList, clipFrame)
            soildisp = os.path.join(scratchfolder, "soil_disp.shp")
            tempclipframe = []
            for i in range(len(soildatalist)):
                soil_disptemp = os.path.join(scratchfolder,"soil_disp_"+str(i)+".shp")
                arcpy.Clip_analysis(soildatalist[i][0],clipFrame,soil_disptemp,"0.000001 DecimalDegrees")
                tempclipframe.append(soil_disptemp)
            arcpy.Merge_management(tempclipframe,soildisp)

            mxd_soil = arcpy.mapping.MapDocument(PSR_CAN_config.mxdfile_soil)
            df_soil = arcpy.mapping.ListDataFrames(mxd_soil,"*")[0]
            df_soil.spatialReference = spatialRef

            # add another column to soil_disp just for symbology purpose
            arcpy.AddField_management(soildisp, "Key", "TEXT", "", "", "", "", "NON_NULLABLE", "REQUIRED", "")
            arcpy.AddField_management(fc_soils_PR, "Key", "TEXT", "", "", "", "", "NON_NULLABLE", "REQUIRED", "")
            keys = []
            fields = arcpy.ListFields(soildisp)
            for field in fields:
                field = field.name
                if field in soil_pkeys:
                    keys.append(field)
            expression= ''
            for key in keys:
                expression = expression +"!"+str(key)+"!"
            arcpy.CalculateField_management(soildisp, 'Key', expression, "PYTHON_9.3")
            arcpy.CalculateField_management(fc_soils_PR, 'Key', expression, "PYTHON_9.3")

            lyr = arcpy.mapping.ListLayers(mxd_soil, "SSURGO*", df_soil)[0]
            lyr.replaceDataSource(scratchfolder,"SHAPEFILE_WORKSPACE", "soil_disp")
            if lyr.symbologyType == "UNIQUE_VALUES":
                lyr.symbology.valueField = 'Key'
                lyr.symbology.addAllValues()
            arcpy.RefreshActiveView()
            arcpy.RefreshTOC()
            soillyr = lyr

            addBuffertoMxd("buffer_soil", df_soil)
            addOrdergeomtoMxd("ordergeoNamePR", df_soil)

            if multipage_soil == False:
                mxd_soil.saveACopy(os.path.join(scratchfolder, "mxd_soil.mxd"))
                arcpy.mapping.ExportToJPEG(mxd_soil, outputjpg_soil, "PAGE_LAYOUT", 480, 640, 200, "False", "24-BIT_TRUE_COLOR", 100)
                if not os.path.exists(os.path.join(PSR_CAN_config.report_path, 'PSRmaps', OrderNumText)):
                    os.mkdir(os.path.join(PSR_CAN_config.report_path, 'PSRmaps', OrderNumText))
                shutil.copy(outputjpg_soil, os.path.join(PSR_CAN_config.report_path, 'PSRmaps', OrderNumText))
                del mxd_soil
                del df_soil

            else:   # multipage
                gridlr = "gridlr_soil"   # gdb feature class doesn't work, could be a bug. So use .shp
                gridlrshp = os.path.join(scratch, gridlr)
                arcpy.GridIndexFeatures_cartography(gridlrshp, bufferSHP_soil, "", "", "", gridsize, gridsize)  # note the tool takes featureclass name only, not the full path


                # part 1: the overview map
                # add grid layer
                gridLayer = arcpy.mapping.Layer(PSR_CAN_config.gridlyrfile)
                gridLayer.replaceDataSource(scratch,"FILEGDB_WORKSPACE","gridlr_soil")
                arcpy.mapping.AddLayer(df_soil,gridLayer,"Top")

                df_soil.extent = gridLayer.getExtent()
                df_soil.scale = df_soil.scale * 1.1

                mxd_soil.saveACopy(os.path.join(scratchfolder, "mxd_soil.mxd"))
                arcpy.mapping.ExportToJPEG(mxd_soil, outputjpg_soil, "PAGE_LAYOUT", 480, 640, 200, "False", "24-BIT_TRUE_COLOR", 100)
                if not os.path.exists(os.path.join(PSR_CAN_config.report_path, 'PSRmaps', OrderNumText)):
                    os.mkdir(os.path.join(PSR_CAN_config.report_path, 'PSRmaps', OrderNumText))
                shutil.copy(outputjpg_soil, os.path.join(PSR_CAN_config.report_path, 'PSRmaps', OrderNumText))
                del mxd_soil
                del df_soil

                # part 2: the data driven pages maps
                page = 1

                page = int(arcpy.GetCount_management(gridlrshp).getOutput(0))  + page
                mxdMM_soil = arcpy.mapping.MapDocument(PSR_CAN_config.mxdMMfile_soil)

                dfMM_soil = arcpy.mapping.ListDataFrames(mxdMM_soil,"*")[0]
                dfMM_soil.spatialReference = spatialRef
                addBuffertoMxd("buffer_soil",dfMM_soil)
                addOrdergeomtoMxd("ordergeoNamePR", dfMM_soil)
                lyr = arcpy.mapping.ListLayers(mxdMM_soil, "SSURGO*", dfMM_soil)[0]
                lyr.replaceDataSource(scratchfolder,"SHAPEFILE_WORKSPACE", "soil_disp")
                if lyr.symbologyType == "UNIQUE_VALUES":
                    lyr.symbology.valueField = "Key"
                    lyr.symbology.addAllValues()
                arcpy.RefreshActiveView()
                arcpy.RefreshTOC()
                soillyr = lyr

                gridlayerMM = arcpy.mapping.ListLayers(mxdMM_soil,"Grid" ,dfMM_soil)[0]
                gridlayerMM.replaceDataSource(scratch, "FILEGDB_WORKSPACE","gridlr_soil")
                arcpy.CalculateAdjacentFields_cartography(gridlrshp, "PageNumber")
                mxdMM_soil.saveACopy(os.path.join(scratchfolder, "mxdMM_soil.mxd"))

                for i in range(1,int(arcpy.GetCount_management(gridlrshp).getOutput(0))+1):
                    arcpy.SelectLayerByAttribute_management(gridlayerMM, "NEW_SELECTION", ' "PageNumber" =  ' + str(i))
                    dfMM_soil.extent = gridlayerMM.getSelectedExtent(True)
                    dfMM_soil.scale = dfMM_soil.scale * 1.1
                    arcpy.SelectLayerByAttribute_management(gridlayerMM, "CLEAR_SELECTION")

                    titleTextE = arcpy.mapping.ListLayoutElements(mxdMM_soil, "TEXT_ELEMENT", "title")[0]
                    titleTextE.text = "Soil Map - Page " + str(i)
                    titleTextE.elementPositionX = 0.6156
                    arcpy.RefreshTOC()

                    arcpy.mapping.ExportToJPEG(mxdMM_soil, outputjpg_soil[0:-4]+str(i)+".jpg", "PAGE_LAYOUT", 480, 640, 200, "False", "24-BIT_TRUE_COLOR", 100)
                    if not os.path.exists(os.path.join(PSR_CAN_config.report_path, 'PSRmaps', OrderNumText)):
                        os.mkdir(os.path.join(PSR_CAN_config.report_path, 'PSRmaps', OrderNumText))
                    shutil.copy(outputjpg_soil[0:-4]+str(i)+".jpg", os.path.join(PSR_CAN_config.report_path, 'PSRmaps', OrderNumText))
                del mxdMM_soil
                del dfMM_soil

            try:
                con = cx_Oracle.connect(PSR_CAN_config.connectionString)
                cur = con.cursor()
                soil_IDs = []
                reportdata = []
                rows = arcpy.SearchCursor(fc_soils_PR)
                for row in rows:
                    reportdata.append([row.Key,row.DS_OID])
                for mapunit in reportdata:
                    erisid = erisid + 1
                    ownerID = str(mapunit[0])
                    ds_oid =str(mapunit[1])
                    eris_data_id = str(cur.callfunc('eris_psr.insertOrderDetailOwnerOid',str, (OrderIDText,ds_oid,ownerID,)))
                    soil_IDs.append([ownerID,eris_data_id])
                print "soil PKey + DS_OID is: "
                print reportdata
                query = cur.callproc('eris_psr.InsertMap', (OrderIDText, 'SOIL', OrderNumText+'_CA_SOIL.jpg', 1))
                if multipage_soil == True:
                    for i in range(1,page):
                        query = cur.callproc('eris_psr.InsertMap', (OrderIDText, 'SOIL', OrderNumText+'_CA_SOIL'+str(i)+'.jpg', i+1))
            finally:
                cur.close()
                con.close()

# 6 Water Wells and Oil and Gas Wells ------------------------------------------------------------------------------------------------------------------------
    if searchRadius !={}:
        print "--- starting WaterWells section " + time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
        in_rows = arcpy.SearchCursor(orderGeometryPR)
        orderCentreSHP = os.path.join(scratchfolder, "SiteMarker.shp")
        point1 = arcpy.Point()
        array1 = arcpy.Array()

        featureList = []
        arcpy.CreateFeatureclass_management(scratchfolder, "SiteMarker.shp", "POINT", "", "DISABLED", "DISABLED", spatialRef)

        cursor = arcpy.InsertCursor(orderCentreSHP)
        feat = cursor.newRow()

        for in_row in in_rows:
            # Set X and Y for start and end points
            point1.X = in_row.xCenUTM
            point1.Y = in_row.yCenUTM
            array1.add(point1)

            centerpoint = arcpy.Multipoint(array1)
            array1.removeAll()
            featureList.append(centerpoint)
            feat.shape = point1
            cursor.insertRow(feat)
        del feat
        del cursor
        del in_row
        del in_rows
        del point1
        del array1

        arcpy.AddField_management(orderCentreSHP, "Lon_X", "DOUBLE", "12", "6", "", "", "NULLABLE", "NON_REQUIRED", "")
        arcpy.AddField_management(orderCentreSHP, "Lat_Y", "DOUBLE", "12", "6", "", "", "NULLABLE", "NON_REQUIRED", "")
        # prepare for elevation calculation
        arcpy.CalculateField_management(orderCentreSHP, 'Lon_X', Lon_X, "PYTHON_9.3", "")
        arcpy.CalculateField_management(orderCentreSHP, 'Lat_Y', Lat_Y, "PYTHON_9.3", "")
        orderCentreSHP = getElevation(orderCentreSHP,["Lon_X","Lat_Y","Id"])
        Call_Google = ''
        rows = arcpy.SearchCursor(orderCentreSHP)
        for row in rows:
            if row.Elevation == -999:
                Call_Google = 'YES'
                break
            else:
                print row.Elevation
        del row
        del rows
        if Call_Google == 'YES':
            arcpy.ImportToolbox (r"\\cabcvan1gis006\GISData\PSR\python\ERIS.tbx")
            orderCentreSHP = arcpy.googleElevation_ERIS(orderCentreSHP).getOutput(0)
        arcpy.AddXY_management(orderCentreSHP)

        mergelist = []
        for dsoid in dsoid_wells:
            bufferSHP_wells = os.path.join(scratchfolder,"buffer_"+dsoid+".shp")
            arcpy.Buffer_analysis(orderGeometryPR, bufferSHP_wells, str(searchRadius[dsoid])+" Kilometer ")
            # arcpy.Buffer_analysis(orderGeometryPR, bufferSHP_wells, "2 Kilometer")
            wells_clip = os.path.join(scratchfolder,'wellsclip_'+dsoid+'.shp')

            arcpy.Clip_analysis(PSR_CAN_config.eris_wells, bufferSHP_wells, wells_clip)
            arcpy.Select_analysis(wells_clip, os.path.join(scratchfolder,'wellsselected_'+dsoid+'.shp'), "DS_OID ="+dsoid)
            mergelist.append(os.path.join(scratchfolder,'wellsselected_'+dsoid+'.shp'))

        wells_merge = os.path.join(scratchfolder, "wells_merge.shp")
        arcpy.Merge_management(mergelist, wells_merge)

        print "--- WaterWells section, after merge " + time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())

        # Calculate Distance with integration and spatial join- can be easily done with Distance tool along with direction if ArcInfo or Advanced license
        wells_mergePR= os.path.join(scratchfolder,"wells_mergePR.shp")
        arcpy.Project_management(wells_merge, wells_mergePR, out_coordinate_system)
        arcpy.Integrate_management(wells_mergePR, ".5 Meters")
        arcpy.AddField_management(orderGeometryPR, "Elevation", "DOUBLE", "12", "6", "", "", "NULLABLE", "NON_REQUIRED", "")
        cursor = arcpy.SearchCursor(orderCentreSHP)
        row = cursor.next()
        elev_marker = row.getValue("Elevation")
        del cursor
        del row
        arcpy.CalculateField_management(orderGeometryPR, 'Elevation', eval(str(elev_marker)), "PYTHON_9.3", "")

        # add distance to selected wells
        wells_sj= os.path.join(scratchfolder,"wells_sj.shp")
        wells_sja= os.path.join(scratchfolder,"wells_sja.shp")
        arcpy.SpatialJoin_analysis(wells_mergePR, orderGeometryPR, wells_sj, "JOIN_ONE_TO_MANY", "KEEP_ALL","#", "CLOSEST","5000 Kilometers", "Distance")   # this is the reported distance
        arcpy.SpatialJoin_analysis(wells_sj, orderCentreSHP, wells_sja, "JOIN_ONE_TO_MANY", "KEEP_ALL","#", "CLOSEST","5000 Kilometers", "Dist_cent")  # this is used for mapkey calculation

        if int(arcpy.GetCount_management(os.path.join(wells_merge)).getOutput(0)) != 0:
            print "--- WaterWells section, exists water wells "
            wells_sja = getElevation(wells_sja,["X","Y","ID"])

            elevationArray=[]
            Call_Google = ''
            rows = arcpy.SearchCursor(wells_sja)
            for row in rows:
                #print row.Elevation
                if row.Elevation == -999:
                    Call_Google = 'YES'
                    break
            del rows

            if Call_Google == 'YES':
                arcpy.ImportToolbox(g_ESRI_variable_14)
                wells_sja = arcpy.googleElevation_ERIS(wells_sja).getOutput(0)

            wells_sja_final= os.path.join(scratchfolder,"wells_sja_PR.shp")
            arcpy.Project_management(wells_sja,wells_sja_final,out_coordinate_system)
            wells_sja = wells_sja_final
            # Add mapkey with script from ERIS toolbox
            arcpy.AddField_management(wells_sja, "MapKeyNo", "SHORT", "", "", "", "", "NULLABLE", "NON_REQUIRED", "")
             # Process: Add Field for mapkey rank storage based on location and total number of keys at one location
            arcpy.AddField_management(wells_sja, "MapKeyLoc", "SHORT", "", "", "", "", "NULLABLE", "NON_REQUIRED", "")
            arcpy.AddField_management(wells_sja, "MapKeyTot", "SHORT", "", "", "", "", "NULLABLE", "NON_REQUIRED", "")
             # Process: Mapkey- to create mapkeys
            calMapkey(wells_sja)

            # Add Direction to ERIS sites
            arcpy.AddField_management(wells_sja, "Direction", "TEXT", "", "", "3", "", "NULLABLE", "NON_REQUIRED", "")
            desc = arcpy.Describe(wells_sja)
            shapefieldName = desc.ShapeFieldName
            rows = arcpy.UpdateCursor(wells_sja)
            for row in rows:
                if(row.Distance<0.001):  # give onsite, give "-" in Direction field
                    directionText = '-'
                else:
                    ref_x = row.xCenUTM      # field is directly accessible
                    ref_y = row.yCenUTM
                    feat = row.getValue(shapefieldName)
                    pnt = feat.getPart()
                    directionText = getDirectionText.getDirectionText(ref_x,ref_y,pnt.X,pnt.Y)

                row.Direction = directionText # field is directly accessible
                rows.updateRow(row)
            del rows

            wells_fin= os.path.join(scratchfolder,"wells_fin.shp")
            arcpy.Select_analysis(wells_sja, wells_fin, '"MapKeyTot" = 1')
            wells_disp= os.path.join(scratchfolder,"wells_disp.shp")
            arcpy.Sort_management(wells_fin, wells_disp, [["MapKeyLoc", "ASCENDING"]])

            arcpy.AddField_management(wells_disp, "Ele_diff", "DOUBLE", "12", "6", "", "", "NULLABLE", "NON_REQUIRED", "")
            arcpy.CalculateField_management(wells_disp, 'Ele_diff', '!Elevation!-!Elevatio_1!', "PYTHON_9.3", "")
            arcpy.AddField_management(wells_disp, "eleRank", "SHORT", "12", "6", "", "", "NULLABLE", "NON_REQUIRED", "")
            arcpy.ImportToolbox (r"\\cabcvan1gis006\GISData\PSR\python\ERIS.tbx")
            arcpy.symbol_ERIS(wells_disp)
            # create a map with water wells and ogw wells
            mxd_wells = arcpy.mapping.MapDocument(PSR_CAN_config.mxdfile_wells)
            df_wells = arcpy.mapping.ListDataFrames(mxd_wells,"*")[0]
            df_wells.spatialReference = spatialRef

            legend = arcpy.mapping.ListLayoutElements(mxd_wells, "LEGEND_ELEMENT", "Legend")[0]
##            if Prov == 'AB':
##                legend.autoAdd = True
##                pipesLayer = arcpy.mapping.Layer(PSR_CAN_config.datalyr_piplineAB)# r"E:\GISData\PSR_CAN\python\mxd\Pipelines_AB.lyr")
##                arcpy.mapping.AddLayer(df_wells,pipesLayer,"Top")
##                pipeInsLayer = arcpy.mapping.Layer( PSR_CAN_config.datalyr_pipInsAB)#"E:\GISData\PSR_CAN\python\mxd\PipelineInstallations_AB.lyr")
##                arcpy.mapping.AddLayer(df_wells,pipeInsLayer,"Top")
            lyr = arcpy.mapping.ListLayers(mxd_wells, "wells", df_wells)[0]
            lyr.replaceDataSource(scratchfolder,"SHAPEFILE_WORKSPACE", "wells_disp")

        else:
            print "--- WaterWells section, no water wells exists "
             ## create a map with water wells and ogw wells
            mxd_wells = arcpy.mapping.MapDocument(PSR_CAN_config.mxdfile_wells)
            df_wells = arcpy.mapping.ListDataFrames(mxd_wells,"*")[0]
            df_wells.spatialReference = spatialRef
            legend = arcpy.mapping.ListLayoutElements(mxd_wells, "LEGEND_ELEMENT", "Legend")[0]
##            if Prov == 'AB':
##                legend.autoAdd = True
##                pipesLayer = arcpy.mapping.Layer(PSR_CAN_config.datalyr_piplineAB)# r"E:\GISData\PSR_CAN\python\mxd\Pipelines_AB.lyr")
##                arcpy.mapping.AddLayer(df_wells,pipesLayer,"Top")
##                pipeInsLayer = arcpy.mapping.Layer( PSR_CAN_config.datalyr_pipInsAB)#"E:\GISData\PSR_CAN\python\mxd\PipelineInstallations_AB.lyr")
##                arcpy.mapping.AddLayer(df_wells,pipeInsLayer,"Top")
##
##        legend.autoAdd = False
        for item in dsoid_wells:
            addBuffertoMxd("buffer_"+item, df_wells)
        df_wells.extent = arcpy.Describe(os.path.join(scratchfolder,"buffer_"+dsoid_wells_maxradius+'.shp')).extent
        df_wells.scale = df_wells.scale * 1.1
        addOrdergeomtoMxd("ordergeoNamePR", df_wells)


        if multipage_wells == False or int(arcpy.GetCount_management(wells_sja).getOutput(0))== 0:
            mxd_wells.saveACopy(os.path.join(scratchfolder, "mxd_wells.mxd"))
            arcpy.mapping.ExportToJPEG(mxd_wells, outputjpg_wells, "PAGE_LAYOUT", 480, 640, 200, "False", "24-BIT_TRUE_COLOR", 100)
            if not os.path.exists(os.path.join(PSR_CAN_config.report_path, 'PSRmaps', OrderNumText)):
                os.mkdir(os.path.join(PSR_CAN_config.report_path, 'PSRmaps', OrderNumText))
            shutil.copy(outputjpg_wells, os.path.join(PSR_CAN_config.report_path, 'PSRmaps', OrderNumText))
            del mxd_wells
            del df_wells
        else:
            gridlr = "gridlr_wells"   # gdb feature class doesn't work, could be a bug. So use .shp
            gridlrshp = os.path.join(scratch, gridlr)
            arcpy.GridIndexFeatures_cartography(gridlrshp, os.path.join(scratchfolder,"buffer_"+dsoid_wells_maxradius+'.shp'), "", "", "", gridsize, gridsize)  #note the tool takes featureclass name only, not the full path

            # part 1: the overview map
            # add grid layer
            gridLayer = arcpy.mapping.Layer(PSR_CAN_config.gridlyrfile)
            gridLayer.replaceDataSource(scratch,"FILEGDB_WORKSPACE",gridlr)
            legend.autoAdd = False
            arcpy.mapping.AddLayer(df_wells,gridLayer,"Top")

            # turn the site label off
            well_lyr = arcpy.mapping.ListLayers(mxd_wells, "wells", df_wells)[0]
            well_lyr.showLabels = False

            df_wells.extent = gridLayer.getExtent()
            df_wells.scale = df_wells.scale * 1.1

            mxd_wells.saveACopy(os.path.join(scratchfolder, "mxd_wells.mxd"))
            arcpy.mapping.ExportToJPEG(mxd_wells, outputjpg_wells, "PAGE_LAYOUT", 480, 640, 200, "False", "24-BIT_TRUE_COLOR", 100)
            if not os.path.exists(os.path.join(PSR_CAN_config.report_path, 'PSRmaps', OrderNumText)):
                os.mkdir(os.path.join(PSR_CAN_config.report_path, 'PSRmaps', OrderNumText))
            shutil.copy(outputjpg_wells, os.path.join(PSR_CAN_config.report_path, 'PSRmaps', OrderNumText))
            del mxd_wells
            del df_wells

            # part 2: the data driven pages
            page = 1

            page = int(arcpy.GetCount_management(gridlrshp).getOutput(0))  + page
            mxdMM_wells = arcpy.mapping.MapDocument(PSR_CAN_config.mxdMMfile_wells)

            dfMM_wells = arcpy.mapping.ListDataFrames(mxdMM_wells)[0]
            dfMM_wells.spatialReference = spatialRef

            for item in dsoid_wells:
                addBuffertoMxd("buffer_"+item, dfMM_wells)

            # addBuffertoMxd("buffer_"+dsoid_wells_maxradius,dfMM_wells)
            addOrdergeomtoMxd("ordergeoNamePR", dfMM_wells)
            gridlayerMM = arcpy.mapping.ListLayers(mxdMM_wells,"Grid" ,dfMM_wells)[0]
            gridlayerMM.replaceDataSource(scratch, "FILEGDB_WORKSPACE","gridlr_wells")
            arcpy.CalculateAdjacentFields_cartography(gridlrshp, "PageNumber")

            lyr = arcpy.mapping.ListLayers(mxdMM_wells, "wells", dfMM_wells)[0]   # "wells" or "Wells" doesn't seem to matter
            lyr.replaceDataSource(scratchfolder,"SHAPEFILE_WORKSPACE", "wells_disp")
##            legend = arcpy.mapping.ListLayoutElements(mxdMM_wells, "LEGEND_ELEMENT", "Legend")[0]
##            if Prov == 'AB':
##                legend.autoAdd = True
##                pipesLayer = arcpy.mapping.Layer(PSR_CAN_config.datalyr_piplineAB)
##                arcpy.mapping.AddLayer(dfMM_wells,pipesLayer,"Top")
##                pipeInsLayer = arcpy.mapping.Layer(PSR_CAN_config.datalyr_pipInsAB)
##                arcpy.mapping.AddLayer(dfMM_wells,pipeInsLayer,"Top")
##
##            legend.autoAdd = False

            for i in range(1,int(arcpy.GetCount_management(gridlrshp).getOutput(0))+1):
                arcpy.SelectLayerByAttribute_management(gridlayerMM, "NEW_SELECTION", ' "PageNumber" =  ' + str(i))
                dfMM_wells.extent = gridlayerMM.getSelectedExtent(True)
                dfMM_wells.scale = dfMM_wells.scale * 1.1
                arcpy.SelectLayerByAttribute_management(gridlayerMM, "CLEAR_SELECTION")

                titleTextE = arcpy.mapping.ListLayoutElements(mxdMM_wells, "TEXT_ELEMENT", "MainTitleText")[0]
                titleTextE.text = "Wells & Additional Sources - Page " + str(i)
                titleTextE.elementPositionX = 0.6438
                arcpy.RefreshTOC()

                arcpy.mapping.ExportToJPEG(mxdMM_wells, outputjpg_wells[0:-4]+str(i)+".jpg", "PAGE_LAYOUT", 480, 640, 200, "False", "24-BIT_TRUE_COLOR",100)
                if not os.path.exists(os.path.join(PSR_CAN_config.report_path, 'PSRmaps', OrderNumText)):
                    os.mkdir(os.path.join(PSR_CAN_config.report_path, 'PSRmaps', OrderNumText))
                shutil.copy(outputjpg_wells[0:-4]+str(i)+".jpg", os.path.join(PSR_CAN_config.report_path, 'PSRmaps', OrderNumText))
            del mxdMM_wells
            del dfMM_wells

        # send wells data to Oracle
        if (int(arcpy.GetCount_management(wells_sja).getOutput(0))== 0):
            # no records selected....
            print 'No well records are selected....'
            try:
                con = cx_Oracle.connect(PSR_CAN_config.connectionString)
                cur = con.cursor()
                query = cur.callproc('eris_psr.InsertMap', (OrderIDText, 'WELLS', OrderNumText+'_CA_WELLS.jpg', 1))          #note type 'SOIL' or 'GEOL' is used internally
                # result = cur.callfunc('eris_psr.CreateReport', str, (OrderIDText,))
            finally:
                cur.close()
                con.close()

        else:
            try:
                con = cx_Oracle.connect(PSR_CAN_config.connectionString)
                cur = con.cursor()
                # cur.callproc('eris_psr.ClearOrder', (OrderIDText,))
                in_rows = arcpy.SearchCursor(wells_sja)
                for in_row in in_rows:
                    erisid_real = in_row.id
                    cur.callproc('eris_psr.InsertOrderDetail', (OrderIDText, erisid_real,in_row.ds_oid,'', in_row.distance, in_row.direction, in_row.elevation, in_row.elevation - in_row.elevatio_1, in_row.mapkeyloc, in_row.mapkeyno))
                del in_row
                del in_rows
                query = cur.callproc('eris_psr.InsertMap', (OrderIDText, 'WELLS', OrderNumText+'_CA_WELLS.jpg', 1))          #note type 'SOIL' or 'GEOL' is used internally
                if multipage_wells == True:
                    for i in range(1,page):
                        query = cur.callproc('eris_psr.InsertMap', (OrderIDText, 'WELLS', OrderNumText+'_CA_WELLS'+str(i)+'.jpg', i+1))
            finally:
                cur.close()
                con.close()

# 7 Radon ------------------------------------------------------------------------------------------------------------------------
    if 'bufferDist_radon' in locals():
        print "--- starting Radon section " + time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
        bufferSHP_radon = os.path.join(scratchfolder,"buffer_radon.shp")
        arcpy.Buffer_analysis(orderGeometryPR, bufferSHP_radon, bufferDist_radon)

        data_radonHR = arcpy.mapping.Layer(PSR_CAN_config.data_radonHR)
        arcpy.SelectLayerByLocation_management(data_radonHR,'intersect',bufferSHP_radon)
        radonHRlist =[]
        pkeys = []

        if(int((arcpy.GetCount_management(data_radonHR).getOutput(0))) !=0):
            rows = arcpy.SearchCursor(data_radonHR)
            for row in rows:
                radonHRlist = ['12885',str(int(row.ID))]
        else:
            print 'no radon from Health Region'

        data_radonPriv = arcpy.mapping.Layer(PSR_CAN_config.data_radonPriv)
        arcpy.SelectLayerByLocation_management(data_radonPriv,'intersect',bufferSHP_radon)
        radonPrivlist =[]
        pkeys = []
        if(int((arcpy.GetCount_management(data_radonPriv).getOutput(0))) !=0):
            rows = arcpy.SearchCursor(data_radonPriv)
            for row in rows:
                radonPrivlist = ['12826',str(int(row.ID))]
        else:
            print 'no radon from Private source'

        try:
            con = cx_Oracle.connect(PSR_CAN_config.connectionString)
            cur = con.cursor()
            for rad in [radonHRlist]+[radonPrivlist]:
                print "radon from HR then private: "
                if rad !=[]:
                    query = cur.callproc('eris_psr.insertOrderDetailOwnerOid',(OrderIDText, rad[0],rad[1]))
                    print query

        finally:
            cur.close()
            con.close()
    else:
        print 'no radon'

# 8 ANSI ------------------------------------------------------------------------------------------------------------------------
    if 'bufferDist_ansi' in locals():
        print "--- starting ANSI section " + time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
        bufferSHP_ansi = os.path.join(scratchfolder,"buffer_ansi.shp")
        arcpy.Buffer_analysis(orderGeometryPR, bufferSHP_ansi, bufferDist_ansi)
        ansidata = arcpy.mapping.Layer(PSR_CAN_config.datalyr_ansi)
        arcpy.SelectLayerByLocation_management(ansidata,'intersect',bufferSHP_ansi)
        if(int((arcpy.GetCount_management(ansidata).getOutput(0))) !=0):
            noANSI = False
            mxd_ansi= arcpy.mapping.MapDocument(PSR_CAN_config.mxdfile_ansi)
            df_ansi = arcpy.mapping.ListDataFrames(mxd_ansi)[0]
            df_ansi.spatialReference = spatialRef

            addBuffertoMxd("buffer_ansi",df_ansi)
            addOrdergeomtoMxd("ordergeoNamePR", df_ansi)

            # print the maps
            if multipage_ansi == False:
                mxd_ansi.saveACopy(os.path.join(scratchfolder, "mxd_ansi.mxd"))
                arcpy.mapping.ExportToJPEG(mxd_ansi, outputjpg_ansi, "PAGE_LAYOUT", 480, 640, 200, "False", "24-BIT_TRUE_COLOR", 100)
                if not os.path.exists(os.path.join(PSR_CAN_config.report_path, 'PSRmaps', OrderNumText)):
                    os.mkdir(os.path.join(PSR_CAN_config.report_path, 'PSRmaps', OrderNumText))
                shutil.copy(outputjpg_ansi, os.path.join(PSR_CAN_config.report_path, 'PSRmaps', OrderNumText))
                del mxd_ansi
                del df_ansi

            else:    # multipage
                gridlr = "gridlr_ansi"   # gdb feature class doesn't work, could be a bug. So use .shp
                gridlrshp = os.path.join(scratch, gridlr)
                arcpy.GridIndexFeatures_cartography(gridlrshp, bufferSHP_ansi, "", "", "", gridsize, gridsize)  # note the tool takes featureclass name only, not the full path

                # part 1: the overview map
                # add grid layer
                gridLayer = arcpy.mapping.Layer(PSR_CAN_config.gridlyrfile)
                gridLayer.replaceDataSource(scratch,"FILEGDB_WORKSPACE","gridlr_ansi")
                arcpy.mapping.AddLayer(df_ansi,gridLayer,"Top")

                df_ansi.extent = gridLayer.getExtent()
                df_ansi.scale = df_ansi.scale * 1.1

                mxd_ansi.saveACopy(os.path.join(scratchfolder, "mxd_ansi.mxd")) 
                arcpy.mapping.ExportToJPEG(mxd_ansi, outputjpg_ansi, "PAGE_LAYOUT", 480, 640, 200, "False", "24-BIT_TRUE_COLOR", 100)
                if not os.path.exists(os.path.join(PSR_CAN_config.report_path, 'PSRmaps', OrderNumText)):
                    os.mkdir(os.path.join(PSR_CAN_config.report_path, 'PSRmaps', OrderNumText))
                shutil.copy(outputjpg_ansi, os.path.join(PSR_CAN_config.report_path, 'PSRmaps', OrderNumText))

                del df_ansi

                # part 2: the data driven pages
                page = 1

                page = int(arcpy.GetCount_management(gridlrshp).getOutput(0))  + page
                mxdMM_ansi = arcpy.mapping.MapDocument(PSR_CAN_config.mxdMMfile_ansi)

                dfMM_ansi = arcpy.mapping.ListDataFrames(mxdMM_ansi)[0]
                dfMM_ansi.spatialReference = spatialRef
                addBuffertoMxd("buffer_ansi",dfMM_ansi)
                addOrdergeomtoMxd("ordergeoNamePR", dfMM_ansi)
                gridlayerMM = arcpy.mapping.ListLayers(mxdMM_ansi,"Grid" ,dfMM_ansi)[0]
                gridlayerMM.replaceDataSource(scratch, "FILEGDB_WORKSPACE","gridlr_ansi")
                arcpy.CalculateAdjacentFields_cartography(gridlrshp, "PageNumber")
                mxdMM_ansi.saveACopy(os.path.join(scratchfolder, "mxdMM_ansi.mxd"))

                for i in range(1,int(arcpy.GetCount_management(gridlrshp).getOutput(0))+1):
                    arcpy.SelectLayerByAttribute_management(gridlayerMM, "NEW_SELECTION", ' "PageNumber" =  ' + str(i))
                    dfMM_ansi.extent = gridlayerMM.getSelectedExtent(True)
                    dfMM_ansi.scale = dfMM_ansi.scale * 1.1
                    arcpy.SelectLayerByAttribute_management(gridlayerMM, "CLEAR_SELECTION")

                    titleTextE = arcpy.mapping.ListLayoutElements(mxdMM_ansi, "TEXT_ELEMENT", "MainTitleText")[0]
                    titleTextE.text = "ANSI Type - Page " + str(i)
                    titleTextE.elementPositionX = 0.468
                    arcpy.RefreshTOC()

                    arcpy.mapping.ExportToJPEG(mxdMM_ansi, outputjpg_ansi[0:-4]+str(i)+".jpg", "PAGE_LAYOUT", 480, 640, 200, "False", "24-BIT_TRUE_COLOR",100)
                    if not os.path.exists(os.path.join(PSR_CAN_config.report_path, 'PSRmaps', OrderNumText)):
                        os.mkdir(os.path.join(PSR_CAN_config.report_path, 'PSRmaps', OrderNumText))
                    shutil.copy(outputjpg_ansi[0:-4]+str(i)+".jpg", os.path.join(PSR_CAN_config.report_path, 'PSRmaps', OrderNumText))
                del mxdMM_ansi
                del dfMM_ansi

            try:
                con = cx_Oracle.connect(PSR_CAN_config.connectionString)
                cur = con.cursor()
                outPolySHP = os.path.join(scratchfolder, "ansiclip.shp")
                arcpy.Clip_analysis(PSR_CAN_config.datalyr_ansi, bufferSHP_ansi, outPolySHP)
                if int(arcpy.GetCount_management(outPolySHP).getOutput(0)) !=0:
                    polygonRows = arcpy.SearchCursor(outPolySHP)
                    ansi_IDs = []
                    ansiTexts =[]
                    for currentPolygonRow in polygonRows:
                        erisid = erisid + 1
                        ansi_IDs.append([str(int(currentPolygonRow.OGF_ID)),erisid])
                        cur.callproc('eris_psr.InsertOrderDetail', (OrderIDText, erisid,PSR_CAN_config.ANSI_ON))
                        query = cur.callproc('eris_psr.InsertFlexRep', (OrderIDText, erisid, PSR_CAN_config.ANSI_ON, 2, 'S1', 1, 'ANSI ID: ' + str(int(currentPolygonRow.OGF_ID)), ''))
                        print query
                        query = cur.callproc('eris_psr.InsertFlexRep', (OrderIDText, erisid, PSR_CAN_config.ANSI_ON, 2, 'N', 2, 'ANSI Name: ', currentPolygonRow.ANSI_NAME))
                        print query
                        query = cur.callproc('eris_psr.InsertFlexRep', (OrderIDText, erisid, PSR_CAN_config.ANSI_ON, 2, 'N', 3, 'Type: ', currentPolygonRow.SUBTYPE))
                        print query
                        query = cur.callproc('eris_psr.InsertFlexRep', (OrderIDText, erisid, PSR_CAN_config.ANSI_ON, 2, 'N', 4, 'Significance: ', currentPolygonRow.SIGNIF))
                        print query
                        query = cur.callproc('eris_psr.InsertFlexRep', (OrderIDText, erisid, PSR_CAN_config.ANSI_ON, 2, 'N', 5, 'Area (sqm): ', currentPolygonRow.SYS_AREA))
                        print query
                        query = cur.callproc('eris_psr.InsertFlexRep', (OrderIDText, erisid, PSR_CAN_config.ANSI_ON, 2, 'N', 6, 'Comments: ', str(currentPolygonRow.GNL_CMT).replace("&","and")))
                        print query
                    del currentPolygonRow
                    del polygonRows

                query = cur.callproc('eris_psr.InsertMap', (OrderIDText, 'ANSI', OrderNumText+'_CA_ANSI.jpg', 1))
                if multipage_ansi == True:
                    for i in range(1,page):
                        query = cur.callproc('eris_psr.InsertMap', (OrderIDText, 'ANSI', OrderNumText+'_CA_ANSI'+str(i)+'.jpg', i+1))

            finally:
                cur.close()
                con.close()
        else:
            noANSI = True

# 9 aspect calculation ------------------------------------------------------------------------------------------------------------------------
    i=0
    imgs = []
    masterLayer_dem = arcpy.mapping.Layer(PSR_CAN_config.masterlyr_dem)
    bufferDistance = '1 KILOMETERS'
    check_field = arcpy.ListFields(orderCentreSHP,"Aspect")
    if len(check_field)==0:
        arcpy.AddField_management(orderCentreSHP, "Aspect",  "TEXT", "", "", "1500", "", "NULLABLE", "NON_REQUIRED", "")
    arcpy.AddXY_management(orderCentreSHP)
    outBufferSHP = os.path.join(scratchfolder, "siteMarker_Buffer.shp")
    arcpy.Buffer_analysis(orderCentreSHP, outBufferSHP, bufferDistance)
    arcpy.DefineProjection_management(outBufferSHP, out_coordinate_system)
    arcpy.SelectLayerByLocation_management(masterLayer_dem, 'intersect', outBufferSHP)

    if (int((arcpy.GetCount_management(masterLayer_dem).getOutput(0)))== 0):
        print "NO records selected for US"
        columns = arcpy.UpdateCursor(orderCentreSHP)
        for column in columns:
            column.Aspect = "Not Available"
        del column
        del columns
        masterLayer_buffer = None

    else:
        # loop through the relevant records, locate the selected cell IDs
        columns = arcpy.SearchCursor(masterLayer_dem)
        for column in columns:
            img = column.getValue("image_name")
            if img ==" ":
                print "no image found"
            else:
                imgs.append(img)
                i = i+1
                print "found img " + img
        del column
        del columns

    if i >=1:

            if i>1:
                clipped_img=''
                n = 1
                for im in imgs:
                    clip_name ="clip_img_"+str(n)+".img"
                    arcpy.Clip_management(os.path.join(PSR_CAN_config.imgdir_dem,im), "#",os.path.join(scratchfolder, clip_name),outBufferSHP,"#","NONE", "MAINTAIN_EXTENT")
                    clipped_img = clipped_img + os.path.join(scratchfolder, clip_name)+ ";"
                    n =n +1

                img = "img.img"
                arcpy.MosaicToNewRaster_management(clipped_img[0:-1],scratchfolder, img,out_coordinate_system, "32_BIT_FLOAT", "#","1", "FIRST", "#")
            elif i ==1:
                im = imgs[0]
                img = "img.img"
                arcpy.Clip_management(os.path.join(PSR_CAN_config.imgdir_dem,im), "#",os.path.join(scratchfolder,img),outBufferSHP,"#","NONE", "MAINTAIN_EXTENT")

            arr =  arcpy.RasterToNumPyArray(os.path.join(scratchfolder,img))

            x,y = gradient(arr)
            slope = 57.29578*arctan(sqrt(x*x + y*y))
            aspect = 57.29578*arctan2(-x,y)

            for i in range(len(aspect)):
                    for j in range(len(aspect[i])):
                        if -180 <=aspect[i][j] <= -90:
                            aspect[i][j] = -90-aspect[i][j]
                        else :
                            aspect[i][j] = 270 - aspect[i][j]
                        if slope[i][j] ==0:
                            aspect[i][j] = -1

            # gather some information on the original file
            spatialref = arcpy.Describe(os.path.join(scratchfolder,img)).spatialReference
            cellsize1  = arcpy.Describe(os.path.join(scratchfolder,img)).meanCellHeight
            cellsize2  = arcpy.Describe(os.path.join(scratchfolder,img)).meanCellWidth
            extent     = arcpy.Describe(os.path.join(scratchfolder,img)).Extent
            pnt        = arcpy.Point(extent.XMin,extent.YMin)

            # save the raster
            aspect_tif = os.path.join(scratchfolder,"aspect.tif")
            aspect_ras = arcpy.NumPyArrayToRaster(aspect,pnt,cellsize1,cellsize2)
            arcpy.CopyRaster_management(aspect_ras,aspect_tif)
            arcpy.DefineProjection_management(aspect_tif, spatialref)


            slope_tif = os.path.join(scratchfolder,"slope.tif")
            slope_ras = arcpy.NumPyArrayToRaster(slope,pnt,cellsize1,cellsize2)
            arcpy.CopyRaster_management(slope_ras,slope_tif)
            arcpy.DefineProjection_management(slope_tif, spatialref)

            aspect_tif_prj = os.path.join(scratchfolder,"aspect_prj.tif")
            arcpy.ProjectRaster_management(aspect_tif,aspect_tif_prj, out_coordinate_system)

            rows = arcpy.da.UpdateCursor(orderCentreSHP,["POINT_X","POINT_Y","Aspect"])
            for row in rows:
                pointX = row[0]
                pointY = row[1]
                location = str(pointX)+" "+str(pointY)
                asp = arcpy.GetCellValue_management(aspect_tif_prj,location)

                if asp.getOutput((0)) != "NoData":
                    asp_text = getDirectionText.dgrDir2txt(float(asp.getOutput((0))))
                    if float(asp.getOutput((0))) == -1:
                        asp_text = r'N/A'
                    row[2] = asp_text
                    print "assign "+asp_text
                    rows.updateRow(row)
                else:
                    print "fail to use point XY to retrieve"
                    row[2] =-9999
                    print "assign -9999"
                    rows.updateRow(row)
                    raise ValueError('No aspect retrieved CHECK data spatial reference')
            del row
            del rows

    in_rows = arcpy.SearchCursor(orderCentreSHP)
    for in_row in in_rows:
        # there is only one line
        site_elev =  in_row.Elevation
        UTM_X = in_row.POINT_X
        UTM_Y = in_row.POINT_Y
        Aspect = in_row.Aspect
    del in_row
    del in_rows

    in_rows = arcpy.SearchCursor(orderGeometryPR)
    for in_row in in_rows:
        # there is only one line
        UTM_Zone = str(in_row.UTM)[32:44]
    del in_row
    del in_rows

    try:
        con = cx_Oracle.connect(PSR_CAN_config.connectionString)
        cur = con.cursor()

        cur.callproc('eris_psr.UpdateOrder', (OrderIDText, UTM_Y, UTM_X, UTM_Zone, site_elev,Aspect))
##        result = cur.callfunc('eris_psr.RunPSR', str, (OrderIDText,))
##        if result == 'Y':
##            print 'report generation success'
##        else:
##            print 'report generation failure'
##            cur.callproc('eris_psr.InsertPSRAudit', (OrderIDText, 'python-RunPSR','Report Failure returned'))

    finally:
        cur.close()
        con.close()
    print "Process completed " + time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())

# 8 XPlorer ------------------------------------------------------------------------------------------------------------------------
    needViewer = 'N'
##    if result == 'Y':
##        if not os.path.exists(PSR_CAN_config.reportcheck_path + r'\\'+OrderNumText+'_CA_PSR.pdf'):
##            time.sleep(10)
##            print 'sleep for ten seconds'
##            arcpy.AddWarning('pdf not there, sleep for ten seconds')
##        shutil.copy(PSR_CAN_config.reportcheck_path + r'\\'+OrderNumText+'_CA_PSR.pdf', scratchfolder)  # occasionally get permission denied issue here when running locally
##        arcpy.SetParameterAsText(1, os.path.join(scratchfolder, OrderNumText+'_CA_PSR.pdf'))
##
##    else:
##        raise ValueError('RunPSR returned "N"')  # this will make the program purposely fail
##        needViewer = 'N'
    try:
        con = cx_Oracle.connect(PSR_CAN_config.connectionString)
        cur = con.cursor()

        cur.execute("select psr_viewer from order_viewer where order_id =" + str(OrderIDText))
        t = cur.fetchone()
        if t != None:
            needViewer = t[0]
    finally:
        cur.close()
        con.close()

    if needViewer == 'Y':
        # clip wetland, flood, geology, soil and covnert .lyr to kml
        # for now, use clipFrame_topo to clip
        # added clip current topo
        viewerdir_kml = os.path.join(scratchfolder,OrderNumText+'_psrkml')
        if not os.path.exists(viewerdir_kml):
            os.mkdir(viewerdir_kml)
        viewerdir_topo = os.path.join(scratchfolder,OrderNumText+'_psrtopo')
        if not os.path.exists(viewerdir_topo):
            os.mkdir(viewerdir_topo)
        viewertemp =os.path.join(scratchfolder,'viewertemp')
        if not os.path.exists(viewertemp):
            os.mkdir(viewertemp)

        viewerdir_relief = os.path.join(scratchfolder,OrderNumText+'_psrrelief')
        if not os.path.exists(viewerdir_relief):
            os.mkdir(viewerdir_relief)

        # Xplorer
        srGoogle = arcpy.SpatialReference(3857)   #web mercator
        srWGS84 = arcpy.SpatialReference(4326)   #WGS84

        # wetland
        if 'bufferDist_wetland' in locals():
            wetlandclip = os.path.join(scratch, "wetlandclip")
            wetlandclip1 = os.path.join(scratch, "wetlandclip1")
            wetlandclip2 = os.path.join(scratch, "wetlandclip_union")
            wetlandlyr = 'wetlandclip_lyr'
            if wetland !='N':
                mxdname = glob.glob(os.path.join(scratchfolder,'mxd_wetland.mxd'))[0]
                mxd = arcpy.mapping.MapDocument(mxdname)
                df = arcpy.mapping.ListDataFrames(mxd,"big")[0]    # the spatial reference here is UTM zone #, need to change to WGS84 Web Mercator
                df.spatialReference = srWGS84
                # re-focus using Buffer layer for multipage
                if multipage_wetland == True:
                    bufferLayer = arcpy.mapping.ListLayers(mxd, "Buffer", df)[0]
                    df.extent = bufferLayer.getSelectedExtent(False)
                    df.scale = df.scale * 1.1

                dfAsFeature = arcpy.Polygon(arcpy.Array([df.extent.lowerLeft, df.extent.lowerRight, df.extent.upperRight, df.extent.upperLeft]),
                                    df.spatialReference)    # df.spatialReference is currently UTM. dfAsFeature is a feature, not even a layer
                del df, mxd
                wetland_boudnary = os.path.join(viewertemp,"Extent_wetland_WGS84.shp")
                arcpy.Project_management(dfAsFeature, wetland_boudnary, srWGS84)
                arcpy.Clip_analysis(PSR_CAN_config.datalyr_wetland, wetland_boudnary, wetlandclip)
                del dfAsFeature

                if int(arcpy.GetCount_management(wetlandclip).getOutput(0)) != 0:
                    arcpy.AddField_management(wetland_boudnary,"WTL_TYPE", "TEXT", "", "", "15", "", "NULLABLE", "NON_REQUIRED", "")
                    arcpy.Union_analysis([wetlandclip,wetland_boudnary],wetlandclip2)
                    arcpy.Project_management(wetlandclip2, wetlandclip1, srWGS84)
                    arcpy.AddField_management(wetlandclip1,"Wetland_Type", "TEXT", "", "", "150", "", "NULLABLE", "NON_REQUIRED", "")
                    rows = arcpy.UpdateCursor(wetlandclip1)
                    for row in rows:
                         row.Wetland_Type = row.WTL_TYPE
                         rows.updateRow(row)
                    arcpy.AddField_management(wetlandclip1,"Name", "TEXT", "", "", "15", "", "NULLABLE", "NON_REQUIRED", "")
                    rows = arcpy.UpdateCursor(wetlandclip1)
                    for row in rows:
                        row.Name = ''
                        rows.updateRow(row)
                    del row
                    del rows
                    keepFieldList = ("Wetland_Type")
                    fieldInfo = ""
                    fieldList = arcpy.ListFields(wetlandclip1)
                    for field in fieldList:
                        if field.name in keepFieldList:
                            if field.name == 'Wetland_Type':
                                fieldInfo = fieldInfo + field.name + " " + "Wetland_Type" + " VISIBLE;"
                            else:
                                pass
                        else:
                            fieldInfo = fieldInfo + field.name + " " + field.name + " HIDDEN;"
                    # print fieldInfo

                    arcpy.MakeFeatureLayer_management(wetlandclip1, wetlandlyr, "", "", fieldInfo[:-1])
                    arcpy.ApplySymbologyFromLayer_management(wetlandlyr, PSR_CAN_config.kmllyr_wetland)
                    arcpy.SaveToLayerFile_management(wetlandlyr, os.path.join(scratchfolder,"wetXX.lyr"), "ABSOLUTE")
                    arcpy.LayerToKML_conversion(wetlandlyr, os.path.join(viewerdir_kml,"wetlandclip.kmz"))
                    arcpy.Delete_management(wetlandlyr)
                else:
                    print ' nodata wetland kml'
                    arcpy.MakeFeatureLayer_management(wetlandclip, wetlandlyr)
                    arcpy.SaveToLayerFile_management(wetlandlyr, os.path.join(scratchfolder,"wetXX_nodata.lyr"), "ABSOLUTE")
                    arcpy.LayerToKML_conversion(wetlandlyr, os.path.join(viewerdir_kml,"wetlandclip_nodata.kmz"))
                    arcpy.Delete_management(wetlandlyr)
            else:
                print "no wetland data, no kml to folder"

# PSW ------------------------------------------------------------------------------------------------------------------------
            pswclip = os.path.join(scratch, "pswclip")
            pswclip1 = os.path.join(scratch, "pswclip1")
            pswclip2 = os.path.join(scratch, "pswclip_union")
            pswlyr = 'pswclip_lyr'
            wetland_boudnary = os.path.join(viewertemp,"Extent_wetland_WGS84.shp")
            if Prov == "NB" or Prov =='ON':
                arcpy.Clip_analysis(PSR_CAN_config.datalyr_psw, wetland_boudnary, pswclip)
                if int(arcpy.GetCount_management(pswclip).getOutput(0)) != 0:
                    arcpy.AddField_management(wetland_boudnary,"PSW", "TEXT", "", "", "15", "", "NULLABLE", "NON_REQUIRED", "")
                    arcpy.Union_analysis([pswclip,wetland_boudnary],pswclip2)
                    arcpy.Project_management(pswclip2, pswclip1, srWGS84)
                    arcpy.AddField_management(pswclip1,"Name", "TEXT", "", "", "15", "", "NULLABLE", "NON_REQUIRED", "")
                    rows = arcpy.UpdateCursor(pswclip1)
                    for row in rows:
                        row.Name = ' '
                        rows.updateRow(row)
                    del row
                    del rows

                    keepFieldList = ("PSW")
                    fieldInfo = ""
                    fieldList = arcpy.ListFields(pswclip1)
                    for field in fieldList:
                        if field.name in keepFieldList:
                            if field.name == 'PSW':
                                fieldInfo = fieldInfo + field.name + " " + "PSW" + " VISIBLE;"
                            else:
                                pass
                        else:
                            fieldInfo = fieldInfo + field.name + " " + field.name + " HIDDEN;"
                    print fieldInfo

                    arcpy.MakeFeatureLayer_management(pswclip1, pswlyr, "", "", fieldInfo[:-1])
                    arcpy.ApplySymbologyFromLayer_management(pswlyr, PSR_CAN_config.kmllyr_psw)
                    arcpy.SaveToLayerFile_management(pswlyr, os.path.join(scratchfolder,"pswXX.lyr"), "ABSOLUTE")
                    arcpy.LayerToKML_conversion(pswlyr, os.path.join(viewerdir_kml,"pswclip.kmz"))
                    arcpy.Delete_management(pswlyr)
                elif wetland !='N':
                    print ' nodata wetland kml'
                    arcpy.MakeFeatureLayer_management(wetlandclip, pswlyr)
                    arcpy.SaveToLayerFile_management(pswlyr, os.path.join(scratchfolder,"pswXX_nodata.lyr"), "ABSOLUTE")
                    arcpy.LayerToKML_conversion(pswlyr, os.path.join(viewerdir_kml,"pswclip_nodata.kmz"))
                    arcpy.Delete_management(pswlyr)
            else:
                print "no psw data, no kml to folder"

# surficial geology ------------------------------------------------------------------------------------------------------------------------
        if 'bufferDist_geol' in locals():
            geologyclipS = os.path.join(scratch, "geologyclipS")
            geologyclipS1 = os.path.join(scratch, "geologyclipS1")
            geologyS_lyr = 'geologyclipS_lyr'
            mxdname = glob.glob(os.path.join(scratchfolder,'mxd_geolS.mxd'))[0]
            mxd = arcpy.mapping.MapDocument(mxdname)
            df = arcpy.mapping.ListDataFrames(mxd,"*")[0]    # the spatial reference here is UTM zone #, need to change to WGS84 Web Mercator
            df.spatialReference = srWGS84
            if multipage_geology == True:
                bufferLayer = arcpy.mapping.ListLayers(mxd, "Buffer", df)[0]
                df.extent = bufferLayer.getSelectedExtent(False)
                df.scale = df.scale * 1.1

            dfAsFeature = arcpy.Polygon(arcpy.Array([df.extent.lowerLeft, df.extent.lowerRight, df.extent.upperRight, df.extent.upperLeft]),
                                df.spatialReference)    #df.spatialReference is currently UTM. dfAsFeature is a feature, not even a layer
            del df, mxd
            arcpy.Project_management(dfAsFeature, os.path.join(viewertemp,"Extent_geolS_WGS84.shp"), srWGS84)
            del dfAsFeature
            geologyclipSs = []
            i =10
            for surficial_lyr in surficialclip_list:
                i+=1
                surficial_data = surficial_lyr[2]
                surficial_lyr = surficial_lyr[3]
                arcpy.Clip_analysis(surficial_lyr, os.path.join(viewertemp,"Extent_geolS_WGS84.shp"),geologyclipS+str(i),"0.000001 DecimalDegrees")
                geologyclipSs.append(geologyclipS+str(i))
            arcpy.Merge_management(geologyclipSs,geologyclipS)
            if arcpy.Describe(geologyclipS).spatialReference.name != srWGS84.name:
                arcpy.Project_management(geologyclipS, geologyclipS1, srWGS84)
            else:
                geologyclipS1 =geologyclipS

            if 'surficial_fields' in locals():
                if int(arcpy.GetCount_management(geologyclipS1).getOutput(0)) != 0:
                    arcpy.AddField_management(geologyclipS1,"ERISBIID", "TEXT", "", "", "150", "", "NULLABLE", "NON_REQUIRED", "")
                    rows = arcpy.UpdateCursor(geologyclipS1)
                    for row in rows:
                        for surficial_fields in surficialclip_list:
                            surficial_fields = surficial_fields[0]
                            surficial_fields = eval("PSR_CAN_config."+surficial_fields+"_surficial")
                            if row.getValue(surficial_fields.keys()[0]) != 'None':
                                ID = [id[1] for id in geologyS_IDs if str(row.getValue(surficial_fields.keys()[0]))==id[0]]
                                if len(ID) ==1:
                                    row.ERISBIID = ID[0]
                                elif ID !=[]:
                                    row.ERISBIID = ID[0]
                                    geologyS_IDs.remove([str(row.getValue(surficial_fields.keys()[0])),ID[0]])
                                rows.updateRow(row)
                    del rows
                    keepFieldList = surficial_fields.keys()
                    keepFieldList.append('ERISBIID')
                    fieldInfo = ""
                    fieldList = arcpy.ListFields(geologyclipS1)
                    for field in fieldList:
                        if field.name in keepFieldList:
                            if field.name =='ERISBIID':
                                fieldInfo = fieldInfo + field.name + " " + "ERISBIID" + " VISIBLE;"
                            else:
                                fieldInfo = fieldInfo + field.name + " " + surficial_fields[field.name] + " VISIBLE;"

                        else:
                            fieldInfo = fieldInfo + field.name + " " + field.name + " HIDDEN;"
                    print fieldInfo

                    arcpy.MakeFeatureLayer_management(geologyclipS1, geologyS_lyr, "", "", fieldInfo[:-1])
                    arcpy.ApplySymbologyFromLayer_management(geologyS_lyr, surficial_lyr)
                    arcpy.SaveToLayerFile_management(geologyS_lyr, os.path.join(scratchfolder,"geoS.lyr"), "ABSOLUTE")
                    arcpy.LayerToKML_conversion(geologyS_lyr, os.path.join(viewerdir_kml,"geologyclipS.kmz"))
                    arcpy.Delete_management(geologyS_lyr)
                else:
                    arcpy.MakeFeatureLayer_management(geologyclipS1, geologyS_lyr)
                    arcpy.SaveToLayerFile_management(geologyS_lyr, os.path.join(scratchfolder,"geoS.lyr"), "ABSOLUTE")
                    arcpy.LayerToKML_conversion(geologyS_lyr, os.path.join(viewerdir_kml,"geologyclipS_nodata.kmz"))
                    arcpy.Delete_management(geologyS_lyr)

            else:

                if int(arcpy.GetCount_management(geologyclipS1).getOutput(0)) != 0:
                    arcpy.AddField_management(geologyclipS1,"ERISBIID", "TEXT", "", "", "150", "", "NULLABLE", "NON_REQUIRED", "")
                    rows = arcpy.UpdateCursor(geologyclipS1)
                    for row in rows:
                        ID = [id[1] for id in geologyS_IDs if row.ULABEL1==id[0]]
                        if ID !=[]:
                            row.ERISBIID = ID[0]
                            rows.updateRow(row)
                    del rows
                    keepFieldList = ("ERISBIID","ULABEL1", "UTYPE1", "HYDRO_INT")
                    fieldInfo = ""
                    fieldList = arcpy.ListFields(geologyclipS1)
                    for field in fieldList:
                        if field.name in keepFieldList:
                            if field.name =='ERISBIID':
                                fieldInfo = fieldInfo + field.name + " " + "ERISBIID" + " VISIBLE;"
                            elif field.name == 'ULABEL1':
                                fieldInfo = fieldInfo + field.name + " " + "Unit_label" + " VISIBLE;"
                            elif field.name == 'UTYPE1':
                                fieldInfo = fieldInfo + field.name + " " + "Unit_type" + " VISIBLE;"
                            elif field.name == 'HYDRO_INT':
                                fieldInfo = fieldInfo + field.name + " " + "Hydro" + " VISIBLE;"
                            else:
                                pass
                        else:
                            fieldInfo = fieldInfo + field.name + " " + field.name + " HIDDEN;"
                    #print fieldInfo
                    arcpy.MakeFeatureLayer_management(geologyclipS1, geologyS_lyr, "", "", fieldInfo[:-1])
                    arcpy.ApplySymbologyFromLayer_management(geologyS_lyr, surficial_lyr)
                    arcpy.SaveToLayerFile_management(geologyS_lyr, os.path.join(scratchfolder,"geoS.lyr"), "ABSOLUTE")
                    arcpy.LayerToKML_conversion(geologyS_lyr, os.path.join(viewerdir_kml,"geologyclipS.kmz"))
                    arcpy.Delete_management(geologyS_lyr)
                elif geolS !='N':
                    arcpy.MakeFeatureLayer_management(geologyclipS1, geologyS_lyr)
                    arcpy.SaveToLayerFile_management(geologyS_lyr, os.path.join(scratchfolder,"geoS_nodata.lyr"), "ABSOLUTE")
                    arcpy.LayerToKML_conversion(geologyS_lyr, os.path.join(viewerdir_kml,"geologyclipS_nodata.kmz"))
                    arcpy.Delete_management(geologyS_lyr)
                else:
                    print "no surficial geology data to kml"

# bedrock geology ------------------------------------------------------------------------------------------------------------------------
            geologyclipB = os.path.join(scratch, "geologyclipB")
            geologyclipB1 = os.path.join(scratch, "geologyclipB1")
            geologyB_lyr = 'geologyclipB_lyr'
            mxdname = glob.glob(os.path.join(scratchfolder,'mxd_geolB.mxd'))[0]
            mxd = arcpy.mapping.MapDocument(mxdname)
            df = arcpy.mapping.ListDataFrames(mxd,"*")[0]    # the spatial reference here is UTM zone #, need to change to WGS84 Web Mercator
            df.spatialReference = srWGS84
            if multipage_geology == True:
                bufferLayer = arcpy.mapping.ListLayers(mxd, "Buffer", df)[0]
                df.extent = bufferLayer.getSelectedExtent(False)
                df.scale = df.scale * 1.1

            dfAsFeature = arcpy.Polygon(arcpy.Array([df.extent.lowerLeft, df.extent.lowerRight, df.extent.upperRight, df.extent.upperLeft]),
                                df.spatialReference)    # df.spatialReference is currently UTM. dfAsFeature is a feature, not even a layer
            del df, mxd
            arcpy.Project_management(dfAsFeature, os.path.join(viewertemp,"Extent_geolB_WGS84.shp"), srWGS84)
            geologyclipBs=[]
            i =0
            for bedrock_lyr in bedrockclip_list:
                i+=1
                bedrock_lyr =bedrock_lyr[3]
                arcpy.Clip_analysis(bedrock_lyr, os.path.join(viewertemp,"Extent_geolB_WGS84.shp"), geologyclipB+str(i))
                geologyclipBs.append(geologyclipB+str(i))
                arcpy.Merge_management(geologyclipBs,geologyclipB)
            if arcpy.Describe(geologyclipB).spatialReference.name != srWGS84.name:
                arcpy.Project_management(geologyclipB, geologyclipB1, srWGS84)
            else:
                geologyclipB1 =geologyclipB
            del dfAsFeature

            if 'bedrock_fields' in locals():
                if int(arcpy.GetCount_management(geologyclipB1).getOutput(0)) != 0:
                    arcpy.AddField_management(geologyclipB1,"ERISBIID", "TEXT", "", "", "150", "", "NULLABLE", "NON_REQUIRED", "")
                    rows = arcpy.UpdateCursor(geologyclipB1)
                    for row in rows:
                        for bedrock_fields in bedrockclip_list:
                            bedrock_fields=bedrock_fields[0]
                            bedrock_fields = eval("PSR_CAN_config."+bedrock_fields+"_bedrock")
                            if str(row.getValue(bedrock_fields.keys()[0]))!= "None":
                                ID = [id[1] for id in geologyB_IDs if str(row.getValue(bedrock_fields.keys()[0]))==id[0]]
                                if ID !=[]:
                                    row.ERISBIID = ID[0]
                                    rows.updateRow(row)
                    del rows
                    keepFieldList = bedrock_fields.keys()
                    keepFieldList.append('ERISBIID')
                    fieldInfo = ""
                    fieldList = arcpy.ListFields(geologyclipB1)
                    for field in fieldList:
                        if field.name in keepFieldList:
                            if field.name =='ERISBIID':
                                fieldInfo = fieldInfo + field.name + " " + "ERISBIID" + " VISIBLE;"
                            else:
                                fieldInfo = fieldInfo + field.name + " " + bedrock_fields[field.name] + " VISIBLE;"
                        else:
                            fieldInfo = fieldInfo + field.name + " " + field.name + " HIDDEN;"
                    print "bedrock ###########################"+fieldInfo
                    arcpy.MakeFeatureLayer_management(geologyclipB1, geologyB_lyr, "", "", fieldInfo[:-1])
                    arcpy.ApplySymbologyFromLayer_management(geologyB_lyr, bedrock_lyr)
                    arcpy.SaveToLayerFile_management(geologyB_lyr, os.path.join(scratchfolder,"geoB.lyr"), "ABSOLUTE")
                    arcpy.LayerToKML_conversion(geologyB_lyr, os.path.join(viewerdir_kml,"geologyclipB.kmz"))
                    arcpy.Delete_management(geologyB_lyr)
                else:
                    arcpy.MakeFeatureLayer_management(geologyclipB1, geologyB_lyr)
                    arcpy.LayerToKML_conversion(geologyB_lyr, os.path.join(viewerdir_kml,"geologyclipB_nodata.kmz"))
                    arcpy.Delete_management(geologyB_lyr)

            else:
                if int(arcpy.GetCount_management(geologyclipB1).getOutput(0)) != 0:
                    arcpy.AddField_management(geologyclipB1,"ERISBIID", "TEXT", "", "", "150", "", "NULLABLE", "NON_REQUIRED", "")
                    rows = arcpy.UpdateCursor(geologyclipB1)
                    for row in rows:
                        ID = [id[1] for id in geologyB_IDs if row.UNIT==id[0]]
                        if ID !=[]:
                            row.ERISBIID = ID[0]
                            rows.updateRow(row)
                    del rows
                    keepFieldList = ("ERISBIID","UNIT", "AGERXTP", "SUBRXTP","GEOLPROV")
                    fieldInfo = ""
                    fieldList = arcpy.ListFields(geologyclipB1)
                    for field in fieldList:
                        if field.name in keepFieldList:
                            if field.name =='ERISBIID':
                                fieldInfo = fieldInfo + field.name + " " + "ERISBIID" + " VISIBLE;"
                            elif field.name == 'UNIT':
                                fieldInfo = fieldInfo + field.name + " " + "Unit_label" + " VISIBLE;"
                            elif field.name == 'AGERXTP':
                                fieldInfo = fieldInfo + field.name + " " + "Unit_type" + " VISIBLE;"
                            elif field.name == 'SUBRXTP':
                                fieldInfo = fieldInfo + field.name + " " + "Hydro" + " VISIBLE;"
                            elif field.name == 'GEOLPROV':
                                fieldInfo = fieldInfo + field.name + " " + "Geological_Province" + " VISIBLE;"
                            else:
                                pass
                        else:
                            fieldInfo = fieldInfo + field.name + " " + field.name + " HIDDEN;"
                    # print fieldInfo
                    arcpy.MakeFeatureLayer_management(geologyclipB1, geologyB_lyr, "", "", fieldInfo[:-1])
                    arcpy.ApplySymbologyFromLayer_management(geologyB_lyr, bedrock_lyr)
                    arcpy.SaveToLayerFile_management(geologyB_lyr, os.path.join(scratchfolder,"geoB.lyr"), "ABSOLUTE")
                    arcpy.LayerToKML_conversion(geologyB_lyr, os.path.join(viewerdir_kml,"geologyclipB.kmz"))
                    arcpy.Delete_management(geologyB_lyr)
                elif geolB !='N':
                    arcpy.MakeFeatureLayer_management(geologyclipB1, geologyB_lyr)
                    arcpy.LayerToKML_conversion(geologyB_lyr, os.path.join(viewerdir_kml,"geologyclipB_nodata.kmz"))
                    arcpy.Delete_management(geologyB_lyr)
                else:
                    print "no geology data to kml"

# soil ------------------------------------------------------------------------------------------------------------------------
        soilclip = os.path.join(scratch,"soilclip")
        soilclip1 = os.path.join(scratch,"soilclip1")
        soilyr = 'soilclip_lyr'
        mxdname = glob.glob(os.path.join(scratchfolder,'mxd_soil.mxd'))[0]
        mxd = arcpy.mapping.MapDocument(mxdname)
        df = arcpy.mapping.ListDataFrames(mxd,"*")[0]    # the spatial reference here is UTM zone #, need to change to WGS84 Web Mercator
        df.spatialReference = srWGS84
        if multipage_soil == True:
            bufferLayer = arcpy.mapping.ListLayers(mxd, "Buffer", df)[0]
            df.extent = bufferLayer.getSelectedExtent(False)
            df.scale = df.scale * 1.1

        dfAsFeature = arcpy.Polygon(arcpy.Array([df.extent.lowerLeft, df.extent.lowerRight, df.extent.upperRight, df.extent.upperLeft]),
                            df.spatialReference)    # df.spatialReference is currently UTM. dfAsFeature is a feature, not even a layer
        del df, mxd
        arcpy.Project_management(dfAsFeature, os.path.join(viewertemp,"Extent_soil_WGS84.shp"), srWGS84)
        arcpy.Clip_analysis(soildisp, os.path.join(viewertemp,"Extent_soil_WGS84.shp"),soilclip)
        if arcpy.Describe(soilclip).spatialReference.name != srWGS84.name:
            arcpy.Project_management(soilclip, soilclip1, srWGS84)
        else:
            soilclip1 =soilclip
        del dfAsFeature
        if int(arcpy.GetCount_management(soilclip1).getOutput(0)) != 0:
            arcpy.AddField_management(soilclip1,"ERISBIID", "TEXT", "", "", "150", "", "NULLABLE", "NON_REQUIRED", "")
            rows = arcpy.UpdateCursor(soilclip1)
            for row in rows:
                ID = [id[1] for id in soil_IDs if row.KEY==id[0]]
                if ID !=[]:
                    row.ERISBIID = ID[0]
                    rows.updateRow(row)
            del rows

            keepFieldList = ("ERISBIID")
            fieldInfo = ""
            fieldList = arcpy.ListFields(soilclip1)
            for field in fieldList:
                if field.name in keepFieldList:
                    if field.name =='ERISBIID':
                        fieldInfo = fieldInfo + field.name + " " + "ERISBIID" + " VISIBLE;"
                    else:
                        pass
                else:
                    fieldInfo = fieldInfo + field.name + " " + field.name + " HIDDEN;"
            # print fieldInfo

            arcpy.MakeFeatureLayer_management(soilclip1, soilyr,"", "", fieldInfo[:-1])
            soilsymbol_copy = os.path.join(scratchfolder,"soillyr_copy.lyr")
            arcpy.SaveToLayerFile_management(soillyr,soilsymbol_copy[:-4])

            arcpy.ApplySymbologyFromLayer_management(soilyr, soilsymbol_copy)
            arcpy.SaveToLayerFile_management(soilyr, os.path.join(scratchfolder,"soilXX.lyr"), "ABSOLUTE")
            arcpy.LayerToKML_conversion(soilyr, os.path.join(viewerdir_kml,"soilclip.kmz"))
            arcpy.Delete_management(soilyr)
        elif soil != 'N':
            arcpy.MakeFeatureLayer_management(soilclip1, soilyr)
            arcpy.SaveToLayerFile_management(soilyr, os.path.join(scratchfolder,"soilXX_nodata.lyr"), "ABSOLUTE")
            arcpy.LayerToKML_conversion(soilyr, os.path.join(viewerdir_kml,"soilclip_nodata.kmz"))
            arcpy.Delete_management(soilyr)
        else:
            print "no soil data to kml"
# ANSI ------------------------------------------------------------------------------------------------------------------------
        # Pipline AB
##        if Prov =='AB':
##            pipeInsLyr = r"pipelineclip_lyr"
##            pipeliclip = os.path.join(scratch,"pipeliclip")
##            pipeliclip1 = os.path.join(scratch,"pipeliclip1")
##            pipeliclip_union = os.path.join(scratch,"pipeliclip2")
##            wetland_boudnary = os.path.join(viewertemp,"Extent_wetland_WGS84.shp")
##            masterLayer_piplineAB = arcpy.mapping.Layer(PSR_CAN_config.datalyr_piplineAB)
##            arcpy.SelectLayerByLocation_management(masterLayer_piplineAB, 'intersect', wetland_boudnary)
##            if int((arcpy.GetCount_management(masterLayer_piplineAB).getOutput(0))) != 0:
##                arcpy.CopyFeatures_management(masterLayer_piplineAB,pipeliclip)
##                fieldList = arcpy.ListFields(pipeliclip)
##                for field in fieldList:
##                        if field.name == 'LICENCE_NO':
##                            arcpy.AlterField_management(pipeliclip, 'LICENCE_NO', 'LICENCE_NO')
##                            fieldInfo = fieldInfo + field.name + " " + "Licence_Number" + " VISIBLE;"
##                        elif  field.name == 'IS_NEB':
##                            arcpy.AlterField_management(pipeliclip, 'IS_NEB', "NEB_Pipeline_Indicator")
##                            fieldInfo = fieldInfo + field.name   + " " + "NEB_Pipeline_Indicator" + " VISIBLE;"
##                        elif field.name == 'LINE_NO':
##                            arcpy.AlterField_management(pipeliclip, 'LINE_NO', "Segment_Line_Number")
##                            fieldInfo = fieldInfo + field.name + " " + "Segment_Line_Number" + " VISIBLE;"
##                        elif field.name == 'LIC_LI_NO':
##                            arcpy.AlterField_management(pipeliclip, 'LIC_LI_NO', "Licence_Line_Number")
##                            fieldInfo = fieldInfo + field.name + " " + "Licence_Line_Number" + " VISIBLE;"
##                        elif field.name == 'PLLICSEGID':
##                            arcpy.AlterField_management(pipeliclip,'PLLICSEGID',"Pipeline_Licence_Segment_Id")
##                            fieldInfo = fieldInfo + field.name + " " + "Pipeline_Licence_Segment_Id" + " VISIBLE;"
##                        elif field.name == 'PL_SPEC_ID':
##                            arcpy.AlterField_management(pipeliclip, 'PL_SPEC_ID',"Pipeline_Specification_Id")
##                            fieldInfo = fieldInfo + field.name + " " + "Pipeline_Specification_Id" + " VISIBLE;"
##                        elif field.name == 'FROM_FAC':
##                            arcpy.AlterField_management(pipeliclip, 'FROM_FAC', 'Segment_From_Facility')
##                            fieldInfo = fieldInfo + field.name + " " + "Segment_From_Facility" + " VISIBLE;"
##                        elif field.name == 'FRM_LOC':
##                            arcpy.AlterField_management(pipeliclip, 'FRM_LOC', 'From_Location')
##                            fieldInfo = fieldInfo + field.name + " " + "From_Location" + " VISIBLE;"
##                        elif field.name == 'TO_FAC':
##                            arcpy.AlterField_management(pipeliclip, 'TO_FAC', 'Segment_To_Facility')
##                            fieldInfo = fieldInfo + field.name  + " " + "Segment_To_Facility" + " VISIBLE;"
##                        elif field.name == 'TO_LOC':
##                            arcpy.AlterField_management(pipeliclip, 'TO_LOC', 'To_Location')
##                            fieldInfo = fieldInfo +field.name+ " " + "To_Location" + " VISIBLE;"
##                        elif field.name == 'H2S_CONTNT':
##                            arcpy.AlterField_management(pipeliclip, 'H2S_CONTNT', 'H2S_Content')
##                            fieldInfo = fieldInfo + field.name  + " " + "H2S_Content" + " VISIBLE;"
##                        elif field.name == 'PIPTECHSTD':
##                            arcpy.AlterField_management(pipeliclip, 'PIPTECHSTD', 'Pipe_Technical_Standard')
##                            fieldInfo = fieldInfo + field.name + " " + "Pipe_Technical_Standard" + " VISIBLE;"
##                        elif field.name == 'OUT_DIAMET':
##                            arcpy.AlterField_management(pipeliclip, 'OUT_DIAMET', 'Pipe_Outside_Diameter')
##                            fieldInfo = fieldInfo + field.name + " " + "Pipe_Outside_Diameter" + " VISIBLE;"
##                        elif field.name == 'WALL_THICK':
##                            arcpy.AlterField_management(pipeliclip, 'WALL_THICK', 'Pipe_Wall_Thickness')
##                            fieldInfo = fieldInfo + field.name + " " + "Pipe_Wall_Thickness" + " VISIBLE;"
##                        elif field.name == 'PIP_MATERL':
##                            arcpy.AlterField_management(pipeliclip, 'PIP_MATERL', 'Pipe_Material')
##                            fieldInfo = fieldInfo + field.name + " " + "Pipe_Material" + " VISIBLE;"
##                        elif field.name == 'PIPE_MAOP':
##                            arcpy.AlterField_management(pipeliclip, 'PIPE_MAOP', 'Pipe_Max_Operating_Pressure')
##                            fieldInfo = fieldInfo + field.name + " " + "Pipe_Max_Operating_Pressure" + " VISIBLE;"
##                        elif field.name == 'JOINTMETHD':
##                            arcpy.AlterField_management(pipeliclip, 'JOINTMETHD', 'Pipe_Joint_Method')
##                            fieldInfo = fieldInfo + field.name  + " " + "Pipe_Joint_Method" + " VISIBLE;"
##                        elif field.name == 'INT_PROTEC':
##                            arcpy.AlterField_management(pipeliclip, 'INT_PROTEC', 'Pipe_Internal_Protection')
##                            fieldInfo = fieldInfo +  field.name + " " + "Pipe_Internal_Protection" + " VISIBLE;"
##                        elif field.name == 'CROSS_TYPE':
##                            arcpy.AlterField_management(pipeliclip, 'CROSS_TYPE', 'Segment_Crossing_Type')
##                            fieldInfo = fieldInfo +  field.name+ " " + "Segment_Crossing_Type" + " VISIBLE;"
##                        elif field.name == 'FLD_CTR_NM':
##                            arcpy.AlterField_management(pipeliclip, 'FLD_CTR_NM', 'Field_Centre')
##                            fieldInfo = fieldInfo +  field.name  + " " + "Field_Centre" + " VISIBLE;"
##                        elif field.name == 'ORIG_LICNO':
##                            arcpy.AlterField_management(pipeliclip, 'ORIG_LICNO', 'Original_Licence_Number')
##                            fieldInfo = fieldInfo +  field.name + " " + "Original_Licence_Number" + " VISIBLE;"
##                        elif field.name == 'ORIGPSPPID':
##                            arcpy.AlterField_management(pipeliclip, 'ORIGPSPPID', 'Original_Pipe_Specification_Id')
##                            fieldInfo = fieldInfo +  field.name + " " + "Original_Pipe_Specification_Id" + " VISIBLE;"
##                        elif field.name == 'LICAPPDATE':
##                            arcpy.AlterField_management(pipeliclip, 'LICAPPDATE', 'Licence_Approval_Date')
##                            fieldInfo = fieldInfo +  field.name + " " + "Licence_Approval_Date" + " VISIBLE;"
##                        elif field.name == 'ORG_ISSUED':
##                            arcpy.AlterField_management(pipeliclip, 'ORG_ISSUED', 'Original_Licence_Issue_Date')
##                            fieldInfo = fieldInfo +  field.name + " " + "Original_Licence_Issue_Date" + " VISIBLE;"
##                        elif field.name == 'PERMT_APPR':
##                            arcpy.AlterField_management(pipeliclip, 'PERMT_APPR', 'Permit_Approval_Date')
##                            fieldInfo = fieldInfo +  field.name + " " + "Permit_Approval_Date" + " VISIBLE;"
##                        elif field.name == 'PERMT_EXPI':
##                            arcpy.AlterField_management(pipeliclip, 'PERMT_EXPI', 'Permit_Expiry_Date')
##                            fieldInfo = fieldInfo +  field.name + " " + "Permit_Expiry_Date" + " VISIBLE;"
##                        elif field.name == 'LAST_OCCYR':
##                            arcpy.AlterField_management(pipeliclip, 'LAST_OCCYR', 'Last_Occurrence_Year')
##                            fieldInfo = fieldInfo +  field.name + " " + "Last_Occurrence_Year" + " VISIBLE;"
##                        elif field.name == 'TEMPSURFPL':
##                            arcpy.AlterField_management(pipeliclip, 'TEMPSURFPL', 'Above_Ground_Pipeline')
##                            fieldInfo = fieldInfo +  field.name + " " + "Above_Ground_Pipeline" + " VISIBLE;"
##                        elif field.name == 'PERMT_EXPI':
##                            arcpy.AlterField_management(pipeliclip, 'PERMT_EXPI', 'Permit_Expiry_Date')
##                            fieldInfo = fieldInfo +  field.name + " " + "Permit_Expiry_Date" + " VISIBLE;"
##                        elif field.name =="Shape_Length":
##                            fieldInfo = fieldInfo +  field.name + " " + field.name + " HIDDEN;"
##                        elif field.name =="GEOM_SRCE":
##                            fieldInfo = fieldInfo +  field.name + " " + field.name + " HIDDEN;"
##                        else:
##                            fieldInfo = fieldInfo +  field.name + " " + field.name + " VISIBLE;"
##
##
##                arcpy.MakeFeatureLayer_management(pipeliclip, pipeInsLyr, "", "", fieldInfo[:-1])
##                arcpy.ApplySymbologyFromLayer_management(pipeInsLyr, PSR_CAN_config.kmldatalyr_piplineAB)
##                arcpy.SaveToLayerFile_management(pipeInsLyr, os.path.join(scratchfolder,"pipelineAB.lyr"), "ABSOLUTE")
##                arcpy.LayerToKML_conversion(pipeInsLyr, os.path.join(viewerdir_kml,"pipelineABclip.kmz"))
##                arcpy.Delete_management(pipeInsLyr)
##            else:
##                print ' nodata wetland kml'
##                arcpy.MakeFeatureLayer_management(pipeliclip, pipeInsLyr)
##                arcpy.SaveToLayerFile_management(pipeInsLyr, os.path.join(scratchfolder,"pipelineAB_nodata.lyr"), "ABSOLUTE")
##                arcpy.LayerToKML_conversion(pipeInsLyr, os.path.join(viewerdir_kml,"pipelineAB_nodata.kmz"))
##                arcpy.Delete_management(pipeInsLyr)
##
##            pipeInsLyr = r"pipeInstclip_lyr"
##            pipeInsclip = os.path.join(scratch,"pipeInsclip")
##            arcpy.Clip_analysis(PSR_CAN_config.datalyr_pipInsAB, wetland_boudnary, pipeInsclip)
##            if int(arcpy.GetCount_management(pipeInsclip).getOutput(0)) != 0:
##                fieldInfo = ""
##                fieldList = arcpy.ListFields(pipeInsclip)
##                for field in fieldList:
##                    if field.name == 'INSTA_LIC':
##                        arcpy.AlterField_management(pipeInsclip, 'INSTA_LIC', 'Pipeline_Licence_Number')
##                        fieldInfo = fieldInfo + field.name + " " + "Pipeline_Licence_Number" + " VISIBLE;"
##                    elif  field.name == 'INSTA_NUM':
##                        arcpy.AlterField_management(pipeInsclip, 'INSTA_NUM', "Pipeline_Installation_ID")
##                        fieldInfo = fieldInfo + field.name   + " " + "Pipeline_Installation_ID" + " VISIBLE;"
##                    elif  field.name == 'INSTA_TYPE':
##                        arcpy.AlterField_management(pipeInsclip, 'INSTA_TYPE', "Installation_Type")
##                        fieldInfo = fieldInfo + field.name   + " " + "Installation_Type" + " VISIBLE;"
##                    elif  field.name == 'PRIME_MOVE':
##                        arcpy.AlterField_management(pipeInsclip, 'PRIME_MOVE', "Prime_Mover")
##                        fieldInfo = fieldInfo + field.name   + " " + "Prime_Mover" + " VISIBLE;"
##                    elif field.name == 'GEOM_SRCE':
##                        arcpy.AlterField_management(pipeInsclip, 'GEOM_SRCE', "Geometry_Source")
##                        fieldInfo = fieldInfo + field.name + " " + "Geometry_Source" + " VISIBLE;"
##                    elif field.name == 'SUBSTANCE':
##                        arcpy.AlterField_management(pipeInsclip, 'SUBSTANCE', "Substance")
##                        fieldInfo = fieldInfo + field.name + " " + "Substance" + " VISIBLE;"
##                    elif field.name == 'LICENCE_DA':
##                        arcpy.AlterField_management(pipeInsclip, 'LICENCE_DA', "Licence_Approval_Date")
##                        fieldInfo = fieldInfo + field.name + " " + "Licence_Approval_Date" + " VISIBLE;"
##                    elif field.name == 'POWER':
##                        arcpy.AlterField_management(pipeInsclip,'POWER',"Installation_Power")
##                        fieldInfo = fieldInfo + field.name + " " + "Installation_Power" + " VISIBLE;"
##                    elif field.name == 'INST_LOCAT':
##                        arcpy.AlterField_management(pipeInsclip, 'INST_LOCAT',"Installation_Location")
##                        fieldInfo = fieldInfo + field.name + " " + "Installation_Location" + " VISIBLE;"
##                    elif field.name == 'FLD_CENTRE':
##                        arcpy.AlterField_management(pipeInsclip, 'FLD_CENTRE',"Field_Centre")
##                        fieldInfo = fieldInfo + field.name + " " + "Field_Centre" + " VISIBLE;"
##                    elif field.name == 'PLINSTATUS':
##                        arcpy.AlterField_management(pipeInsclip, 'PLINSTATUS',"Pipeline_Installation_Status")
##                        fieldInfo = fieldInfo + field.name + " " + "Pipeline_Installation_Status" + " VISIBLE;"
##                    elif field.name == 'ORIGINSTNO':
##                        arcpy.AlterField_management(pipeInsclip, 'ORIGINSTNO',"Original_Installation_Number")
##                        fieldInfo = fieldInfo + field.name + " " + "Original_Installation_Number" + " VISIBLE;"
##                    else:
##                        fieldInfo = fieldInfo +  field.name + " " + field.name + " VISIBLE;"
##
##                arcpy.MakeFeatureLayer_management(pipeInsclip, pipeInsLyr, "", "", fieldInfo[:-1])
##                arcpy.ApplySymbologyFromLayer_management(pipeInsLyr, PSR_CAN_config.datalyr_pipInsAB)
##                arcpy.SaveToLayerFile_management(pipeInsLyr, os.path.join(scratchfolder,"pipeInstAB.lyr"), "ABSOLUTE")
##                arcpy.LayerToKML_conversion(pipeInsLyr, os.path.join(viewerdir_kml,"pipeInstABclip.kmz"))
##                arcpy.Delete_management(pipeInsLyr)
##            else:
##                    print ' nodata wetland kml'
##                    arcpy.MakeFeatureLayer_management(pipeInsclip, pipeInsLyr)
##                    arcpy.SaveToLayerFile_management(pipeInsLyr, os.path.join(scratchfolder,"pipelineAB_nodata.lyr"), "ABSOLUTE")
##                    arcpy.LayerToKML_conversion(pipeInsLyr, os.path.join(viewerdir_kml,"pipelineAB_nodata.kmz"))
##                    arcpy.Delete_management(pipeInsLyr)
        if Prov =='AB':
            bufferDistance = '0.5 KiloMeter'
            bufferSHP_pipe = os.path.join(scratch, "pipebuffer")
            pipeInsLyr = r"pipelineclip_lyr"
            pipeliclip = os.path.join(scratch,"pipeliclip")
##            arcpy.Clip_analysis(PSR_CAN_config.datalyr_piplineAB, wetland_boudnary, pipeliclip)
            masterLayer_piplineAB = arcpy.mapping.Layer(PSR_CAN_config.datalyr_piplineAB)
            pipeliclip = os.path.join(scratch,"final_clip_pipe")
            arcpy.Buffer_analysis(orderGeometryPR, bufferSHP_pipe, bufferDistance)
            arcpy.SelectLayerByLocation_management(masterLayer_piplineAB, 'intersect', bufferSHP_pipe)
            if int((arcpy.GetCount_management(masterLayer_piplineAB).getOutput(0))) != 0:
                arcpy.CopyFeatures_management(masterLayer_piplineAB,pipeliclip)

                keepFieldList = ("LICENCE_NO",)
                fieldInfo = ""
                fieldList = arcpy.ListFields(pipeliclip)
                for field in fieldList:
                    if field.name == 'LICENCE_NO':
                        arcpy.AlterField_management(pipeliclip, 'LICENCE_NO', 'LICENCE_NO')
                        fieldInfo = fieldInfo + field.name + " " + "Licence_Number" + " VISIBLE;"
                    elif  field.name == 'IS_NEB':
                        arcpy.AlterField_management(pipeliclip, 'IS_NEB', "NEB_Pipeline_Indicator")
                        fieldInfo = fieldInfo + field.name   + " " + "NEB_Pipeline_Indicator" + " VISIBLE;"
                    elif  field.name == 'COMP_NAME':
                        arcpy.AlterField_management(pipeliclip, 'COMP_NAME', "Company_Name")
                        fieldInfo = fieldInfo + field.name   + " " + "Company_Name" + " VISIBLE;"
                    elif field.name == 'LINE_NO':
                        arcpy.AlterField_management(pipeliclip, 'LINE_NO', "Segment_Line_Number")
                        fieldInfo = fieldInfo + field.name + " " + "Segment_Line_Number" + " VISIBLE;"
                    elif field.name == 'SEG_LENGTH':
                        arcpy.AlterField_management(pipeliclip, 'SEG_LENGTH', "Segment_Length")
                        fieldInfo = fieldInfo + field.name + " " + "Segment_Length" + " VISIBLE;"
                    elif field.name == 'SEG_STATUS':
                        arcpy.AlterField_management(pipeliclip, 'SEG_STATUS', "Segment_Status")
                        fieldInfo = fieldInfo + field.name + " " + "Segment_Length" + " VISIBLE;"
                    elif field.name == 'LIC_LI_NO':
                        arcpy.AlterField_management(pipeliclip, 'LIC_LI_NO', "Licence_Line_Number")
                        fieldInfo = fieldInfo + field.name + " " + "Licence_Line_Number" + " VISIBLE;"
                    elif field.name == 'PLLICSEGID':
                        arcpy.AlterField_management(pipeliclip,'PLLICSEGID',"Pipeline_Licence_Segment_Id")
                        fieldInfo = fieldInfo + field.name + " " + "Pipeline_Licence_Segment_Id" + " VISIBLE;"
                    elif field.name == 'PL_SPEC_ID':
                        arcpy.AlterField_management(pipeliclip, 'PL_SPEC_ID',"Pipeline_Specification_Id")
                        fieldInfo = fieldInfo + field.name + " " + "Pipeline_Specification_Id" + " VISIBLE;"
                    elif field.name == 'FROM_FAC':
                        arcpy.AlterField_management(pipeliclip, 'FROM_FAC', 'Segment_From_Facility')
                        fieldInfo = fieldInfo + field.name + " " + "Segment_From_Facility" + " VISIBLE;"
                    elif field.name == 'FRM_LOC':
                        arcpy.AlterField_management(pipeliclip, 'FRM_LOC', 'From_Location')
                        fieldInfo = fieldInfo + field.name + " " + "From_Location" + " VISIBLE;"
                    elif field.name == 'TO_FAC':
                        arcpy.AlterField_management(pipeliclip, 'TO_FAC', 'Segment_To_Facility')
                        fieldInfo = fieldInfo + field.name  + " " + "Segment_To_Facility" + " VISIBLE;"
                    elif field.name == 'TO_LOC':
                        arcpy.AlterField_management(pipeliclip, 'TO_LOC', 'To_Location')
                        fieldInfo = fieldInfo +field.name+ " " + "To_Location" + " VISIBLE;"
                    elif field.name == 'H2S_CONTNT':
                        arcpy.AlterField_management(pipeliclip, 'H2S_CONTNT', 'H2S_Content')
                        fieldInfo = fieldInfo + field.name  + " " + "H2S_Content" + " VISIBLE;"
                    elif field.name == 'PIPTECHSTD':
                        arcpy.AlterField_management(pipeliclip, 'PIPTECHSTD', 'Pipe_Technical_Standard')
                        fieldInfo = fieldInfo + field.name + " " + "Pipe_Technical_Standard" + " VISIBLE;"
                    elif field.name == 'OUT_DIAMET':
                        arcpy.AlterField_management(pipeliclip, 'OUT_DIAMET', 'Pipe_Outside_Diameter')
                        fieldInfo = fieldInfo + field.name + " " + "Pipe_Outside_Diameter" + " VISIBLE;"
                    elif field.name == 'WALL_THICK':
                        arcpy.AlterField_management(pipeliclip, 'WALL_THICK', 'Pipe_Wall_Thickness')
                        fieldInfo = fieldInfo + field.name + " " + "Pipe_Wall_Thickness" + " VISIBLE;"
                    elif field.name == 'PIP_MATERL':
                        arcpy.AlterField_management(pipeliclip, 'PIP_MATERL', 'Pipe_Material')
                        fieldInfo = fieldInfo + field.name + " " + "Pipe_Material" + " VISIBLE;"
                    elif field.name == 'PIPE_MAOP':
                        arcpy.AlterField_management(pipeliclip, 'PIPE_MAOP', 'Pipe_Max_Operating_Pressure')
                        fieldInfo = fieldInfo + field.name + " " + "Pipe_Max_Operating_Pressure" + " VISIBLE;"
                    elif field.name == 'JOINTMETHD':
                        arcpy.AlterField_management(pipeliclip, 'JOINTMETHD', 'Pipe_Joint_Method')
                        fieldInfo = fieldInfo + field.name  + " " + "Pipe_Joint_Method" + " VISIBLE;"
                    elif field.name == 'INT_PROTEC':
                        arcpy.AlterField_management(pipeliclip, 'INT_PROTEC', 'Pipe_Internal_Protection')
                        fieldInfo = fieldInfo +  field.name + " " + "Pipe_Internal_Protection" + " VISIBLE;"
                    elif field.name == 'CROSS_TYPE':
                        arcpy.AlterField_management(pipeliclip, 'CROSS_TYPE', 'Segment_Crossing_Type')
                        fieldInfo = fieldInfo +  field.name+ " " + "Segment_Crossing_Type" + " VISIBLE;"
                    elif field.name == 'FLD_CTR_NM':
                        arcpy.AlterField_management(pipeliclip, 'FLD_CTR_NM', 'Field_Centre')
                        fieldInfo = fieldInfo +  field.name  + " " + "Field_Centre" + " VISIBLE;"
                    elif field.name == 'ORIG_LICNO':
                        arcpy.AlterField_management(pipeliclip, 'ORIG_LICNO', 'Original_Licence_Number')
                        fieldInfo = fieldInfo +  field.name + " " + "Original_Licence_Number" + " VISIBLE;"
                    elif field.name == 'ORIGPSPPID':
                        arcpy.AlterField_management(pipeliclip, 'ORIGPSPPID', 'Original_Pipe_Specification_Id')
                        fieldInfo = fieldInfo +  field.name + " " + "Original_Pipe_Specification_Id" + " VISIBLE;"
                    elif field.name == 'LICAPPDATE':
                        arcpy.AlterField_management(pipeliclip, 'LICAPPDATE', 'Licence_Approval_Date')
                        fieldInfo = fieldInfo +  field.name + " " + "Licence_Approval_Date" + " VISIBLE;"
                    elif field.name == 'ORG_ISSUED':
                        arcpy.AlterField_management(pipeliclip, 'ORG_ISSUED', 'Original_Licence_Issue_Date')
                        fieldInfo = fieldInfo +  field.name + " " + "Original_Licence_Issue_Date" + " VISIBLE;"
                    elif field.name == 'PERMT_APPR':
                        arcpy.AlterField_management(pipeliclip, 'PERMT_APPR', 'Permit_Approval_Date')
                        fieldInfo = fieldInfo +  field.name + " " + "Permit_Approval_Date" + " VISIBLE;"
                    elif field.name == 'PERMT_EXPI':
                        arcpy.AlterField_management(pipeliclip, 'PERMT_EXPI', 'Permit_Expiry_Date')
                        fieldInfo = fieldInfo +  field.name + " " + "Permit_Expiry_Date" + " VISIBLE;"
                    elif field.name == 'LAST_OCCYR':
                        arcpy.AlterField_management(pipeliclip, 'LAST_OCCYR', 'Last_Occurrence_Year')
                        fieldInfo = fieldInfo +  field.name + " " + "Last_Occurrence_Year" + " VISIBLE;"
                    elif field.name == 'TEMPSURFPL':
                        arcpy.AlterField_management(pipeliclip, 'TEMPSURFPL', 'Above_Ground_Pipeline')
                        fieldInfo = fieldInfo +  field.name + " " + "Above_Ground_Pipeline" + " VISIBLE;"
                    elif field.name == 'PERMT_EXPI':
                        arcpy.AlterField_management(pipeliclip, 'PERMT_EXPI', 'Permit_Expiry_Date')
                        fieldInfo = fieldInfo +  field.name + " " + "Permit_Expiry_Date" + " VISIBLE;"
                    elif field.name =="Shape_Length":
                        fieldInfo = fieldInfo +  field.name + " " + field.name + " HIDDEN;"
                    elif field.name =="GEOM_SRCE":
                        fieldInfo = fieldInfo +  field.name + " " + field.name + " HIDDEN;"
                    else:
                        fieldInfo = fieldInfo +  field.name + " " + field.name + " VISIBLE;"

                arcpy.MakeFeatureLayer_management(pipeliclip, pipeInsLyr, "", "", fieldInfo[:-1])
                arcpy.ApplySymbologyFromLayer_management(pipeInsLyr, PSR_CAN_config.kmldatalyr_piplineAB)
                #arcpy.SaveToLayerFile_management(pipeInsLyr, os.path.join(scratchfolder,"pipelineAB.lyr"), "ABSOLUTE")
                arcpy.LayerToKML_conversion(pipeInsLyr, os.path.join(viewerdir_kml,"pipelineABclip.kmz"))
                arcpy.Delete_management(pipeInsLyr)
            else:
                arcpy.CopyFeatures_management(masterLayer_piplineAB,pipeliclip)
                print ' nodata pipeInstclip_lyr kml'
                arcpy.MakeFeatureLayer_management(pipeliclip, pipeInsLyr)
                #arcpy.SaveToLayerFile_management(pipeInsLyr, os.path.join(scratchfolder,"pipelineAB_nodata.lyr"), "ABSOLUTE")
                arcpy.LayerToKML_conversion(pipeInsLyr, os.path.join(viewerdir_kml,"pipelineAB_nodata.kmz"))
                arcpy.Delete_management(pipeInsLyr)

            pipeInsLyr = r"pipeInstclip_lyr"
            pipeInsclip = os.path.join(scratch,"pipeInsclip")
            arcpy.Clip_analysis(PSR_CAN_config.datalyr_pipInsAB, bufferSHP_pipe, pipeInsclip)
            if int(arcpy.GetCount_management(pipeInsclip).getOutput(0)) != 0:
                fieldInfo = ""
                fieldList = arcpy.ListFields(pipeInsclip)
                for field in fieldList:
                 try:
                    if field.name == 'INSTA_LIC':
                        arcpy.AlterField_management(pipeInsclip, 'INSTA_LIC', 'Pipeline_Licence_Number')
                        fieldInfo = fieldInfo + field.name + " " + "Pipeline_Licence_Number" + " VISIBLE;"
                    elif  field.name == 'INSTA_NUM':
                        arcpy.AlterField_management(pipeInsclip, 'INSTA_NUM', "Pipeline_Installation_ID")
                        fieldInfo = fieldInfo + field.name   + " " + "Pipeline_Installation_ID" + " VISIBLE;"
                    elif  field.name == 'INSTA_TYPE':
                        arcpy.AlterField_management(pipeInsclip, 'INSTA_TYPE', "Installation_Type")
                        fieldInfo = fieldInfo + field.name   + " " + "Installation_Type" + " VISIBLE;"
                    elif  field.name == 'PRIME_MOVE':
                        arcpy.AlterField_management(pipeInsclip, 'PRIME_MOVE', "Prime_Mover")
                        fieldInfo = fieldInfo + field.name   + " " + "Prime_Mover" + " VISIBLE;"
                    elif field.name == 'GEOM_SRCE':
                        arcpy.AlterField_management(pipeInsclip, 'GEOM_SRCE', "Geometry_Source")
                        fieldInfo = fieldInfo + field.name + " " + "Geometry_Source" + " VISIBLE;"
                    elif field.name == 'SUBSTANCE':
                        arcpy.AlterField_management(pipeInsclip, 'SUBSTANCE', "Substance")
                        fieldInfo = fieldInfo + field.name + " " + "Substance" + " VISIBLE;"
                    elif field.name == 'LICENCE_DA':
                        arcpy.AlterField_management(pipeInsclip, 'LICENCE_DA', "Licence_Approval_Date")
                        fieldInfo = fieldInfo + field.name + " " + "Licence_Approval_Date" + " VISIBLE;"
                    elif field.name == 'POWER':
                        arcpy.AlterField_management(pipeInsclip,'POWER',"Installation_Power")
                        fieldInfo = fieldInfo + field.name + " " + "Installation_Power" + " VISIBLE;"
                    elif field.name == 'INST_LOCAT':
                        arcpy.AlterField_management(pipeInsclip, 'INST_LOCAT',"Installation_Location")
                        fieldInfo = fieldInfo + field.name + " " + "Installation_Location" + " VISIBLE;"
                    elif field.name == 'FLD_CENTRE':
                        arcpy.AlterField_management(pipeInsclip, 'FLD_CENTRE',"Field_Centre")
                        fieldInfo = fieldInfo + field.name + " " + "Field_Centre" + " VISIBLE;"
                    elif field.name == 'PLINSTATUS':
                        arcpy.AlterField_management(pipeInsclip, 'PLINSTATUS',"Pipeline_Installation_Status")
                        fieldInfo = fieldInfo + field.name + " " + "Pipeline_Installation_Status" + " VISIBLE;"
                    elif field.name == 'ORIGINSTNO':
                        arcpy.AlterField_management(pipeInsclip, 'ORIGINSTNO',"Original_Installation_Number")
                        fieldInfo = fieldInfo + field.name + " " + "Original_Installation_Number" + " VISIBLE;"
                    else:
                        fieldInfo = fieldInfo +  field.name + " " + field.name + " VISIBLE;"
                 except:
                    continue
                arcpy.MakeFeatureLayer_management(pipeInsclip, pipeInsLyr, "", "", fieldInfo[:-1])
                arcpy.ApplySymbologyFromLayer_management(pipeInsLyr, PSR_CAN_config.datalyr_pipInsAB)
                #arcpy.SaveToLayerFile_management(pipeInsLyr, os.path.join(scratchfolder,"pipeInstAB.lyr"), "ABSOLUTE")
                arcpy.LayerToKML_conversion(pipeInsLyr, os.path.join(viewerdir_kml,"pipefacilityABclip.kmz"))
                arcpy.Delete_management(pipeInsLyr)

            else:
                print ' nodata pipeInstclip_lyr kml'
                arcpy.CopyFeatures_management(masterLayer_piplineAB,pipeliclip)
                arcpy.MakeFeatureLayer_management(pipeInsclip, pipeInsLyr)
                #arcpy.SaveToLayerFile_management(pipeInsLyr, os.path.join(scratchfolder,"pipelineAB_nodata.lyr"), "ABSOLUTE")
                arcpy.LayerToKML_conversion(pipeInsLyr, os.path.join(viewerdir_kml,"pipefacilityAB_nodata.kmz"))
                arcpy.Delete_management(pipeInsLyr)
        elif Prov == 'SK':
            bufferDistance = '0.5 KiloMeter'
            bufferSHP_pipe = os.path.join(scratch, "pipebuffer")
            pipeInsLyr = r"pipelineclip_lyr"
            pipeliclip = os.path.join(scratch,"pipeliclip")
            # arcpy.Clip_analysis(PSR_CAN_config.datalyr_piplineAB, wetland_boudnary, pipeliclip)
            masterLayer_piplineSK = arcpy.mapping.Layer(PSR_CAN_config.datalyr_pipelineSK)
            pipeliclip = os.path.join(scratch,"final_clip_pipe")
            arcpy.Buffer_analysis(orderGeometryPR, bufferSHP_pipe, bufferDistance)
            arcpy.SelectLayerByLocation_management(masterLayer_piplineSK, 'intersect', bufferSHP_pipe)
            if int((arcpy.GetCount_management(masterLayer_piplineSK).getOutput(0))) != 0:
                arcpy.CopyFeatures_management(masterLayer_piplineSK,pipeliclip)
                arcpy.MakeFeatureLayer_management(pipeliclip, pipeInsLyr)
                arcpy.ApplySymbologyFromLayer_management(pipeInsLyr, PSR_CAN_config.kmldatalyr_pipelineSK)
                #arcpy.SaveToLayerFile_management(pipeInsLyr, os.path.join(scratchfolder,"pipelineAB.lyr"), "ABSOLUTE")
                arcpy.LayerToKML_conversion(pipeInsLyr, os.path.join(viewerdir_kml,"pipelineSKclip.kmz"))
                arcpy.Delete_management(pipeInsLyr)
            else:
                arcpy.CopyFeatures_management(masterLayer_piplineSK,pipeliclip)
                print ' nodata pipeInstclip_lyr kml'
                arcpy.MakeFeatureLayer_management(pipeliclip, pipeInsLyr)
                #arcpy.SaveToLayerFile_management(pipeInsLyr, os.path.join(scratchfolder,"pipelineAB_nodata.lyr"), "ABSOLUTE")
                arcpy.LayerToKML_conversion(pipeInsLyr, os.path.join(viewerdir_kml,"pipelineSK_nodata.kmz"))
                arcpy.Delete_management(pipeInsLyr)
        elif Prov == 'BC':
            bufferDistance = '0.5 KiloMeter'
            bufferSHP_pipe = os.path.join(scratch, "pipebuffer")
            pipeInsLyr = r"pipelineclip_lyr"
            pipeliclip = os.path.join(scratch,"pipeliclip")
            # arcpy.Clip_analysis(PSR_CAN_config.datalyr_piplineAB, wetland_boudnary, pipeliclip)
            masterLayer_piplineBC = arcpy.mapping.Layer(PSR_CAN_config.datalyr_pipelineBC)
            pipeliclip = os.path.join(scratch,"final_clip_pipe")
            arcpy.Buffer_analysis(orderGeometryPR, bufferSHP_pipe, bufferDistance)
            arcpy.SelectLayerByLocation_management(masterLayer_piplineBC, 'intersect', bufferSHP_pipe)
            if int((arcpy.GetCount_management(masterLayer_piplineBC).getOutput(0))) != 0:
                arcpy.CopyFeatures_management(masterLayer_piplineBC,pipeliclip)
                arcpy.MakeFeatureLayer_management(pipeliclip, pipeInsLyr)
                arcpy.ApplySymbologyFromLayer_management(pipeInsLyr, PSR_CAN_config.kmldatalyr_pipelineBC)
                #arcpy.SaveToLayerFile_management(pipeInsLyr, os.path.join(scratchfolder,"pipelineAB.lyr"), "ABSOLUTE")
                arcpy.LayerToKML_conversion(pipeInsLyr, os.path.join(viewerdir_kml,"pipelineBCclip.kmz"))
                arcpy.Delete_management(pipeInsLyr)
            else:
                arcpy.CopyFeatures_management(masterLayer_piplineBC,pipeliclip)
                print ' nodata pipeInstclip_lyr kml'
                arcpy.MakeFeatureLayer_management(pipeliclip, pipeInsLyr)
                #arcpy.SaveToLayerFile_management(pipeInsLyr, os.path.join(scratchfolder,"pipelineAB_nodata.lyr"), "ABSOLUTE")
                arcpy.LayerToKML_conversion(pipeInsLyr, os.path.join(viewerdir_kml,"pipelineBC_nodata.kmz"))
                arcpy.Delete_management(pipeInsLyr)

            bufferDistance = '0.5 KiloMeter'
            bufferSHP_pipe = os.path.join(scratch, "pipebuffer")
            pipeInsLyr = r"pipelinerow_lyr"
            pipeliclip = os.path.join(scratch,"pipelirow")
            # arcpy.Clip_analysis(PSR_CAN_config.datalyr_piplineAB, wetland_boudnary, pipeliclip)
            masterLayer_piplineBC = arcpy.mapping.Layer(PSR_CAN_config.datalyr_pipelineROWBC)
            pipeliclip = os.path.join(scratch,"final_clip_row")
            arcpy.Buffer_analysis(orderGeometryPR, bufferSHP_pipe, bufferDistance)
            arcpy.SelectLayerByLocation_management(masterLayer_piplineBC, 'intersect', bufferSHP_pipe)
            if int((arcpy.GetCount_management(masterLayer_piplineBC).getOutput(0))) != 0:
                arcpy.CopyFeatures_management(masterLayer_piplineBC,pipeliclip)
                arcpy.MakeFeatureLayer_management(pipeliclip, pipeInsLyr)
                arcpy.ApplySymbologyFromLayer_management(pipeInsLyr, PSR_CAN_config.kmldatalyr_pipelineROWBC)
                #arcpy.SaveToLayerFile_management(pipeInsLyr, os.path.join(scratchfolder,"pipelineAB.lyr"), "ABSOLUTE")
                arcpy.LayerToKML_conversion(pipeInsLyr, os.path.join(viewerdir_kml,"pipelineBCROWclip.kmz"))
                arcpy.Delete_management(pipeInsLyr)
            else:
                arcpy.CopyFeatures_management(masterLayer_piplineBC,pipeliclip)
                print ' nodata pipeInstclip_lyr kml'
                arcpy.MakeFeatureLayer_management(pipeliclip, pipeInsLyr)
                #arcpy.SaveToLayerFile_management(pipeInsLyr, os.path.join(scratchfolder,"pipelineAB_nodata.lyr"), "ABSOLUTE")
                arcpy.LayerToKML_conversion(pipeInsLyr, os.path.join(viewerdir_kml,"pipelineBCROW_nodata.kmz"))
                arcpy.Delete_management(pipeInsLyr)

        # ansi
        if Topo == 'ON':
            ansiclip = os.path.join(scratch, "ansiclip")
            ansiclip1 = os.path.join(scratch, "ansiclip1")
            ansiclip2 = os.path.join(scratch, "ansiclip_PR")
            ansilyr = 'ansiclip_lyr'
            if noANSI ==False:
                mxdname = glob.glob(os.path.join(scratchfolder,'mxd_ansi.mxd'))[0]
                mxd = arcpy.mapping.MapDocument(mxdname)
                df = arcpy.mapping.ListDataFrames(mxd,"big")[0]    # the spatial reference here is UTM zone #, need to change to WGS84 Web Mercator
                df.spatialReference = srWGS84

                if multipage_ansi == True:
                    bufferLayer = arcpy.mapping.ListLayers(mxd, "Buffer", df)[0]
                    df.extent = bufferLayer.getSelectedExtent(False)
                    df.scale = df.scale * 1.1

                dfAsFeature = arcpy.Polygon(arcpy.Array([df.extent.lowerLeft, df.extent.lowerRight, df.extent.upperRight, df.extent.upperLeft]),
                                    df.spatialReference)    #df.spatialReference is currently UTM. dfAsFeature is a feature, not even a layer
                del df, mxd
                ansi_boudnary = os.path.join(viewertemp,"Extent_ansi_WGS84.shp")
                arcpy.Project_management(dfAsFeature, ansi_boudnary, srWGS84)
                arcpy.Clip_analysis(PSR_CAN_config.datalyr_ansi, ansi_boudnary, ansiclip)
                if arcpy.Describe(ansiclip).spatialReference.name != srWGS84.name:
                    arcpy.Project_management(ansiclip, ansiclip1, srWGS84)
                else:
                    ansiclip1 =ansiclip
                del dfAsFeature

                if int(arcpy.GetCount_management(ansiclip1).getOutput(0)) != 0:
                    arcpy.AddField_management(ansi_boudnary,"SIGNIF", "TEXT", "", "", "15", "", "NULLABLE", "NON_REQUIRED", "")
                    arcpy.Union_analysis([ansiclip1,ansi_boudnary],ansiclip2)
                    arcpy.AddField_management(ansiclip2,"ERISBIID", "TEXT", "", "", "150", "", "NULLABLE", "NON_REQUIRED", "")
                    rows = arcpy.UpdateCursor(ansiclip2)
                    for row in rows:
                        ID = [id[1] for id in ansi_IDs if str(int(row.OGF_ID))==id[0]]
                        if ID !=[]:
                            row.ERISBIID = ID[0]
                            rows.updateRow(row)
                    del row
                    del rows
                    arcpy.AddField_management(ansiclip2,"Name", "TEXT", "", "", "15", "", "NULLABLE", "NON_REQUIRED", "")
                    rows = arcpy.UpdateCursor(ansiclip2)
                    for row in rows:
                        row.Name = ' '
                        rows.updateRow(row)
                    del row
                    del rows
                    fieldInfo = ""
                    fieldList = arcpy.ListFields(ansiclip2)
                    for field in fieldList:
                            if field.name == 'ANSI_NAME':
                                fieldInfo = fieldInfo + field.name + " " + "ANSI_NAME" + " VISIBLE;"
                            elif field.name == 'SUBTYPE':
                                fieldInfo = fieldInfo + field.name + " " + "SUBTYPE" + " VISIBLE;"
                            elif field.name == 'SIGNIF':
                                fieldInfo = fieldInfo + field.name + " " + "SIGNIF" + " VISIBLE;"
                            elif field.name == 'MGMT_PLAN':
                                fieldInfo = fieldInfo + field.name + " " + "MGMT_PLAN" + " VISIBLE;"
                            elif field.name == 'SYS_AREA':
                                fieldInfo = fieldInfo + field.name + " " + "SYS_AREA" + " VISIBLE;"
                            elif field.name == 'GNL_CMT':
                                fieldInfo = fieldInfo + field.name + " " + "GNL_CMT" + " VISIBLE;"
                            elif field.name == 'ERISBIID':
                                fieldInfo = fieldInfo + field.name + " " + "GNL_CMT" + " VISIBLE;"
                            else:
                                fieldInfo = fieldInfo + field.name + " " + field.name + " HIDDEN;"

                    arcpy.MakeFeatureLayer_management(ansiclip2, ansilyr, "", "", fieldInfo[:-1])
                    arcpy.ApplySymbologyFromLayer_management(ansilyr, PSR_CAN_config.datalyr_ansi)
                    arcpy.SaveToLayerFile_management(ansilyr, os.path.join(scratchfolder,"ansiXX.lyr"), "ABSOLUTE")
                    arcpy.LayerToKML_conversion(ansilyr, os.path.join(viewerdir_kml,"ansiclip.kmz"))
                    arcpy.Delete_management(ansilyr)
                else:
                    arcpy.MakeFeatureLayer_management(ansiclip1, ansilyr)
                    arcpy.LayerToKML_conversion(ansilyr, os.path.join(viewerdir_kml,"ansiclip_nodata.kmz"))
                    arcpy.Delete_management(ansilyr)

# current topo clipping for Xplorer ------------------------------------------------------------------------------------------------------------------------
        imagename = r"topoclip.jpg"
        mxdname = glob.glob(os.path.join(scratchfolder,'mxd_topo.mxd'))[0]
        mxd = arcpy.mapping.MapDocument(mxdname)
        df = arcpy.mapping.ListDataFrames(mxd,"*")[0]    # the spatial reference here is UTM zone #, need to change to WGS84 Web Mercator
        df.spatialReference = srGoogle
        bufferLayer = arcpy.mapping.ListLayers(mxd, "Buffer", df)[0]
        df.extent = bufferLayer.getSelectedExtent()
        if multipage_topo == True:
            df.extent = bufferLayer.getSelectedExtent(False)
            df.scale = df.scale * 1.1
        dfAsFeature = arcpy.Polygon(arcpy.Array([df.extent.lowerLeft, df.extent.lowerRight, df.extent.upperRight, df.extent.upperLeft]),
                            df.spatialReference)
        arcpy.Project_management(dfAsFeature, os.path.join(viewertemp,"Extent_topo_WGS84.shp"), srWGS84)
        del dfAsFeature

        if Topo =='ON' or Topo =='NS':
            for lyr in arcpy.mapping.ListLayers(mxd, "", df):
                if lyr.name == "SiteMaker" or lyr.name =='Project Property':
                    arcpy.mapping.RemoveLayer(df, lyr)
            arcpy.mapping.ExportToJPEG(mxd, os.path.join(viewerdir_topo,imagename), df, df_export_width=1950,df_export_height=2000, world_file = True, resolution = 150)
            arcpy.DefineProjection_management(os.path.join(viewerdir_topo,imagename), srGoogle)
            extent = arcpy.Describe(os.path.join(viewerdir_topo,imagename)).Extent.projectAs(srWGS84)

            metaitem = {}
            metaitem['type'] = 'capsrtopo'
            metaitem['imagename'] = imagename
            metaitem['lat_sw'] = extent.YMin
            metaitem['long_sw'] = extent.XMin
            metaitem['lat_ne'] = extent.YMax
            metaitem['long_ne'] = extent.XMax

            print 'Topo Xplorer metadata:'
            print metaitem
            try:
                con = cx_Oracle.connect(PSR_CAN_config.connectionString)
                cur = con.cursor()

                cur.execute("delete from overlay_image_info where  order_id = %s and (type = 'capsrtopo')" % str(OrderIDText))
                cur.execute("insert into overlay_image_info values (%s, %s, %s, %.5f, %.5f, %.5f, %.5f, %s, '', '')" % (str(OrderIDText), str(OrderNumText), "'" + metaitem['type']+"'", metaitem['lat_sw'], metaitem['long_sw'], metaitem['lat_ne'], metaitem['long_ne'],"'"+metaitem['imagename']+"'" ) )
                con.commit()

            finally:
                cur.close()
                con.close()
        elif topofiles != []:
            tomosaiclist = []
            n = 0
            for topotif in topofiles:
                try:
                    arcpy.Clip_management(topotif,"",os.path.join(viewertemp, "topo"+str(n)+".jpg"),os.path.join(viewertemp,"Extent_topo_WGS84.shp"),"255","ClippingGeometry")
                    tomosaiclist.append(os.path.join(viewertemp, "topo"+str(n)+".jpg"))
                    n = n+1
                except Exception, e:
                    print str(e)     # possibly not in the clipframe
            arcpy.MosaicToNewRaster_management(tomosaiclist, viewerdir_topo,imagename,srGoogle,"","","3","MINIMUM","MATCH")
            desc = arcpy.Describe(os.path.join(viewerdir_topo, imagename))
            featbound = arcpy.Polygon(arcpy.Array([desc.extent.lowerLeft, desc.extent.lowerRight, desc.extent.upperRight, desc.extent.upperLeft]),
                                desc.spatialReference)
            del desc
            tempfeat = os.path.join(scratchfolder, "imgbnd_"+imagename[:-4]+ ".shp")

            arcpy.Project_management(featbound, tempfeat, srWGS84) # function requires output not be in_memory
            del featbound
            desc = arcpy.Describe(tempfeat)
            metaitem = {}
            metaitem['type'] = 'capsrtopo'
            metaitem['imagename'] = imagename
            metaitem['lat_sw'] = desc.extent.YMin
            metaitem['long_sw'] = desc.extent.XMin
            metaitem['lat_ne'] = desc.extent.YMax
            metaitem['long_ne'] = desc.extent.XMax
            print 'Topo Xplorer metadata:'
            print metaitem

            try:
                con = cx_Oracle.connect(PSR_CAN_config.connectionString)
                cur = con.cursor()

                cur.execute("delete from overlay_image_info where  order_id = %s and (type = 'capsrtopo')" % str(OrderIDText))
                cur.execute("insert into overlay_image_info values (%s, %s, %s, %.5f, %.5f, %.5f, %.5f, %s, '', '')" % (str(OrderIDText), str(OrderNumText), "'" + metaitem['type']+"'", metaitem['lat_sw'], metaitem['long_sw'], metaitem['lat_ne'], metaitem['long_ne'],"'"+metaitem['imagename']+"'" ) )
                con.commit()

            finally:
                cur.close()
                con.close()
        elif Topo == 'N':
                print "no topo data, check error"
        del df, mxd

# clip relief map ------------------------------------------------------------------------------------------------------------------------
        tomosaiclist = []
        imagename = "relief.jpg"
        n = 0
        if relief != 'N' and img_selected!=[]:
            for item in img_selected:
                try:
                    arcpy.Clip_management(os.path.join(scratchfolder,item),"",os.path.join(viewertemp, "relief"+str(n)+".jpg"),os.path.join(viewertemp,"Extent_topo_WGS84.shp"),"255","ClippingGeometry")
                    tomosaiclist.append(os.path.join(viewertemp, "relief"+str(n)+".jpg"))
                    n = n+1
                except Exception, e:
                    print str(e) + item     # possibly not in the clipframe
            arcpy.MosaicToNewRaster_management(tomosaiclist, viewerdir_relief,imagename,srGoogle,"","","1","MINIMUM","MATCH")
            desc = arcpy.Describe(os.path.join(viewerdir_relief, imagename))
            featbound = arcpy.Polygon(arcpy.Array([desc.extent.lowerLeft, desc.extent.lowerRight, desc.extent.upperRight, desc.extent.upperLeft]),desc.spatialReference)
            del desc
            tempfeat = os.path.join(scratchfolder, "imgbnd_"+imagename[:-4]+ ".shp")
            arcpy.Project_management(featbound, tempfeat, srWGS84) # function requires output not be in_memory
            del featbound
            desc = arcpy.Describe(tempfeat)

            metaitem = {}
            metaitem['type'] = 'psrrelief'
            metaitem['imagename'] = imagename
            metaitem['lat_sw'] = desc.extent.YMin
            metaitem['long_sw'] = desc.extent.XMin
            metaitem['lat_ne'] = desc.extent.YMax
            metaitem['long_ne'] = desc.extent.XMax
            print 'Relief Xplorer metadata:'
            print metaitem

            try:
                con = cx_Oracle.connect(PSR_CAN_config.connectionString)
                cur = con.cursor()
                cur.execute("delete from overlay_image_info where  order_id = %s and (type = 'psrrelief')" % str(OrderIDText))
                cur.execute("insert into overlay_image_info values (%s, %s, %s, %.5f, %.5f, %.5f, %.5f, %s, '', '')" % (str(OrderIDText), str(OrderNumText), "'" + metaitem['type']+"'", metaitem['lat_sw'], metaitem['long_sw'], metaitem['lat_ne'], metaitem['long_ne'],"'"+metaitem['imagename']+"'" ) )
                con.commit()
            finally:
                cur.close()
                con.close()

# clip contour lines ------------------------------------------------------------------------------------------------------------------------
        contourclip = os.path.join(scratch, "contourclip")
        contourclip1 = os.path.join(scratch, "contourclip1")
        contourlyr = 'contourclip_lyr'
        arcpy.Clip_analysis(PSR_CAN_config.datalyr_contour, os.path.join(viewertemp,"Extent_topo_WGS84.shp"), contourclip)
        if arcpy.Describe(contourclip).spatialReference.name != srWGS84.name:
                arcpy.Project_management(contourclip, contourclip1, srWGS84)
        else:
                contourclip1 =contourclip
        if int(arcpy.GetCount_management(contourclip1).getOutput(0)) != 0:
            arcpy.AddField_management(contourclip1,"Name", "TEXT", "", "", "15", "", "NULLABLE", "NON_REQUIRED", "")
            rows = arcpy.UpdateCursor(contourclip1)
            for row in rows:
                row.Name = 'Elevation'
                rows.updateRow(row)
            arcpy.AddField_management(contourclip1,"Name", "TEXT", "", "", "15", "", "NULLABLE", "NON_REQUIRED", "")
            rows = arcpy.UpdateCursor(contourclip1)
            for row in rows:
                row.Name = ' '
                rows.updateRow(row)
            del row
            del rows
            keepFieldList = ("elevation")
            fieldInfo = ""
            fieldList = arcpy.ListFields(contourclip1)
            for field in fieldList:
                if field.name in keepFieldList:
                    if field.name == 'elevation':
                        fieldInfo = fieldInfo + field.name + " " + "elevation" + " VISIBLE;"
                    else:
                        pass
                else:
                    fieldInfo = fieldInfo + field.name + " " + field.name + " HIDDEN;"
            # print fieldInfo

            arcpy.MakeFeatureLayer_management(contourclip1, contourlyr, "", "", fieldInfo[:-1])
            arcpy.ApplySymbologyFromLayer_management(contourlyr, PSR_CAN_config.datalyr_contour)
            arcpy.SaveToLayerFile_management(contourlyr, os.path.join(scratchfolder,"contourXX.lyr"), "ABSOLUTE")
            arcpy.LayerToKML_conversion(contourlyr, os.path.join(viewerdir_relief,"contourclip.kmz"))
            arcpy.Delete_management(contourlyr)
        else:
            print "no contour data, no kml to folder"
            arcpy.MakeFeatureLayer_management(contourclip1, contourlyr)
            arcpy.LayerToKML_conversion(contourlyr, os.path.join(viewerdir_relief,"contourclip_nodata.kmz"))
            arcpy.Delete_management(contourlyr)

        if os.path.exists(os.path.join(PSR_CAN_config.viewer_path, OrderNumText+"_capsrkml")):
            shutil.rmtree(os.path.join(PSR_CAN_config.viewer_path, OrderNumText+"_capsrkml"))
        shutil.copytree(viewerdir_kml, os.path.join(PSR_CAN_config.viewer_path, OrderNumText+"_capsrkml"))
        url = PSR_CAN_config.upload_link + "CAPSRKMLUpload?ordernumber=" + OrderNumText
        urllib.urlopen(url)

        if os.path.exists(os.path.join(PSR_CAN_config.viewer_path, OrderNumText+"_capsrtopo")):
            shutil.rmtree(os.path.join(PSR_CAN_config.viewer_path, OrderNumText+"_capsrtopo"))
        shutil.copytree(viewerdir_topo, os.path.join(PSR_CAN_config.viewer_path, OrderNumText+"_capsrtopo"))
        url = PSR_CAN_config.upload_link + "CAPSRTOPOUpload?ordernumber=" + OrderNumText
        urllib.urlopen(url)

        if os.path.exists(os.path.join(PSR_CAN_config.viewer_path, OrderNumText+"_psrrelief")):
            shutil.rmtree(os.path.join(PSR_CAN_config.viewer_path, OrderNumText+"_psrrelief"))
        shutil.copytree(viewerdir_relief, os.path.join(PSR_CAN_config.viewer_path, OrderNumText+"_psrrelief"))
        url = PSR_CAN_config.upload_link + "ReliefUpload?ordernumber=" + OrderNumText
        urllib.urlopen(url)
    arcpy.SetParameterAsText(1, os.path.join(scratchfolder, OrderNumText+'_CA_PSR.pdf'))
except:
    # Get the traceback object
    tb = sys.exc_info()[2]
    tbinfo = traceback.format_tb(tb)[0]

    # Concatenate information together concerning the error into a message string
    pymsg = "PYTHON ERRORS:\nTraceback info:\n" + tbinfo + "\nError Info:\n" + str(sys.exc_info()[1])
    msgs = "ArcPy ERRORS:\n" + arcpy.GetMessages(2) + "\n"


    try:
        con = cx_Oracle.connect(PSR_CAN_config.connectionString)
        #con = cx_Oracle.connect('eris/eris@GMTEST.glaciermedia.inc')
        cur = con.cursor()
        #cur.callproc('eris_psr.ClearOrder', (OrderIDText,))
        ##cur.callproc('eris_psr.InsertPSRAudit', (OrderIDText, 'python-Error Handling',pymsg))

    finally:
        cur.close()
        con.close()

    # Return python error messages for use in script tool or Python Window
    arcpy.AddError("hit CC's error code in except: order ID: %s"%OrderIDText)
    arcpy.AddError(pymsg)
    arcpy.AddError(msgs)

    # Print Python error messages for use in Python / Python Window
    print pymsg + "\n"
    print msgs
    raise    # raise the error again

# print ("Final PSR report directory: " + (str(PSR_CAN_config.report_path + "\\PSRmaps\\" + OrderNumText)))
# print("DONE!")