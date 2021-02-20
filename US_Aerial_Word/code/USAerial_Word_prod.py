#-------------------------------------------------------------------------------
# Name:        US Aerial report for US_Word
# Purpose:     create US Aerial report in US_Word required Word format
#
# Author:      jliu
#
# Created:     06/03/2017
# Copyright:
# Licence:
#-------------------------------------------------------------------------------

# Use Texas provided Aerial files
# export geometry seperately in emf format
# save picture/vector to the location which links to the word template
#modify text in word template

import time,json
print "#0 " + time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())

import arcpy, os, win32com
import csv, cx_Oracle
import xml.etree.ElementTree as ET
import operator
import shutil, zipfile
import logging,traceback
from win32com import client
from time import strftime

def goCoverPage(coverInfo):
     global summary_mxdfile
     global scratch
     mxd = arcpy.mapping.MapDocument(Covermxdfile)
     SITENAME = arcpy.mapping.ListLayoutElements(mxd, "TEXT_ELEMENT", "siteText")[0]
     SITENAME.text = coverInfo["SITE_NAME"]
     ADDRESS = arcpy.mapping.ListLayoutElements(mxd, "TEXT_ELEMENT", "addressText")[0]
     ADDRESS.text = coverInfo["ADDRESS"]
     PROJECT_NUM = arcpy.mapping.ListLayoutElements(mxd, "TEXT_ELEMENT", "projectidText")[0]
     PROJECT_NUM.text = coverInfo["PROJECT_NUM"]
     COMPANY_NAME = arcpy.mapping.ListLayoutElements(mxd, "TEXT_ELEMENT", "companyText")[0]
     COMPANY_NAME.text = coverInfo["COMPANY_NAME"]
     ORDER_NUM = arcpy.mapping.ListLayoutElements(mxd, "TEXT_ELEMENT", "ordernumText")[0]
     ORDER_NUM.text = coverInfo["ORDER_NUM"]
     coverPDF = os.path.join(scratch, "coverpage.emf")
     arcpy.mapping.ExportToEMF(mxd, coverPDF, "PAGE_LAYOUT")
     mxd.saveACopy(os.path.join(scratch,"coverpage.mxd"))
     return coverPDF

# create WORD and also make a copy of the geotiff files if the scale is too small
def createWORD(app):
    infomatrix = []
    directory = os.path.join(viewer_path, OrderNumText + '_aerial')
    i = 0
    items = os.listdir(directory)
    for i in range(0,len(items)):
        item = items[i]
        print item
        if item.lower() == 'thumbs.db' or item.lower() == '_aux' or item.lower() == '_bndry' or item.lower() == '_del' or "jpg" not in item[-4:]:
            continue

        year = item.split('.')[0].split('_')[0]
        source = item.split('.')[0].split('_')[1]
        scale = int(item.replace("tiftojpg","").split('.')[0].split('_')[2])*12
        if len(item.split('.')[0].split('_')) > 3:
            comment = item.split('.')[0].split('_')[3]
        else:
            comment = ''
        infomatrix.append((item, year, source, scale, comment))


    if OrderType.lower()== 'point':
        orderGeomlyrfile = orderGeomlyrfile_point
    elif OrderType.lower() =='polyline':
        orderGeomlyrfile = orderGeomlyrfile_polyline
    else:
        orderGeomlyrfile = orderGeomlyrfile_polygon

    orderGeomLayer = arcpy.mapping.Layer(orderGeomlyrfile)
    orderGeomLayer.replaceDataSource(scratch,"SHAPEFILE_WORKSPACE","orderGeometry")


    if os.path.exists(os.path.join(scratch,'tozip')):
        shutil.rmtree(os.path.join(scratch,'tozip'))
    shutil.copytree(directorytemplate,os.path.join(scratch,'tozip'))

    # add to map template, clip (but need to keep both metadata: year, grid size, quadrangle name(s) and present in order
    mxd = arcpy.mapping.MapDocument(mxdfile)
    df = arcpy.mapping.ListDataFrames(mxd,"*")[0]
    spatialRef = arcpy.SpatialReference(out_coordinate_system)
    df.spatialReference = spatialRef
    # if OrderType.lower() == "polyline" or OrderType.lower() == "polygon":
    #    arcpy.mapping.AddLayer(df,orderGeomLayer,"Top")
    if OrderType.lower() == "polyline" or OrderType.lower() == "polygon":
        if yesBoundary.lower() == 'y' or yesBoundary.lower() == 'yes':
            arcpy.mapping.AddLayer(df,orderGeomLayer,"Top")

    infomatrix_sorted = sorted(infomatrix,key=operator.itemgetter(1,0), reverse = True)
    for i in range(0,len(infomatrix_sorted)):
        imagename = infomatrix_sorted[i][0]
        year = infomatrix_sorted[i][1]
        source = infomatrix_sorted[i][2]
        scale = infomatrix_sorted[i][3]
        comment = infomatrix_sorted[i][4]

##        for lyr in arcpy.mapping.ListLayers(mxd, "Project Property", df):
##            if lyr.name == "Project Property":
##                if OrderType.lower() == "point":
##                    lyr.visible = False
##                else:
##                    lyr.visible = True
##                df.extent = lyr.getSelectedExtent(False)

        for lyr in arcpy.mapping.ListLayers(mxd, "Project Property", df):
            if lyr.name == "Project Property":
                if OrderType.lower() == "point":
                    lyr.visible = False
                else:
                    lyr.visible = True

        centerlyr = arcpy.mapping.Layer(orderCenter)
        # centerlyr = arcpy.MakeFeatureLayer_management(orderCenter, "center_lyr")
        arcpy.mapping.AddLayer(df,centerlyr,"Top")


        center = arcpy.mapping.ListLayers(mxd, "*", df)[0]
        df.extent = center.getSelectedExtent(False)
        center.visible = False


        df.scale = scale
        arcpy.RefreshTOC()
        arcpy.RefreshActiveView()

        outputemf = os.path.join(scratch, year+".emf")
        print outputemf
        arcpy.mapping.ExportToEMF(mxd, outputemf, "PAGE_LAYOUT")

        shutil.copyfile(os.path.join(directory,imagename), os.path.join(scratch,"tozip\word\media\image2.jpeg"))
        shutil.copyfile(os.path.join(scratch,year+".emf"), os.path.join(scratch,"tozip\word\media\image1.emf"))
        zipdir_noroot(os.path.join(scratch,'tozip'),year+".docx")
        worddoclist.append(os.path.join(scratch,year+".docx"))

        # the word template has been copied, the image files have also been copied, need to refresh and replace the text fields, save
        doc = app.Documents.Open(os.path.join(scratch,year+".docx"))

        fileName = OrderNumText
        fileDate = time.strftime('%Y-%m-%d', time.localtime())

        # quads = 'AERIAL PHOTOGRAPHY FROM SOURCE ' + source + '(' +str(year) + ')'
        quads = ''

        if scale == 6000:
            scaletxt = '1":' + str(scale/12)+"'"
        # scaletxt = '1:' + str(scale)
        else:
            scaletxt = '1:' + str(scale)

        allShapes = doc.Shapes
        allShapes(3).TextFrame.TextRange.Text = 'AERIAL PHOTO (' + str(year) + '-' + source+')'    #AERIAL PHOTOGRAPH line

        txt = allShapes(4).TextFrame.TextRange.Text.replace('Site Name', siteName)
        # allShapes(4).TextFrame.TextRange.Text.replace('Site Address', siteAddress)
        # allShapes(4).TextFrame.TextRange.Text.replace('Site City, Site State', siteCityState)
        txt = txt.replace('Site Address', siteAddress)
        txt = txt.replace('Site City, Site State', siteCityState)
        allShapes(4).TextFrame.TextRange.Text = txt
        allShapes(9).TextFrame.TextRange.Text = quads
        allShapes(11).TextFrame.TextRange.Text = officeAddress
        allShapes(12).TextFrame.TextRange.Text = officeCity
        allShapes(13).TextFrame.TextRange.Text = proNo
        allShapes(26).TextFrame.TextRange.Text = scaletxt
        allShapes(27).TextFrame.TextRange.Text = fileName
        allShapes(28).TextFrame.TextRange.Text = fileDate
        doc.Save()
        doc.Close()
        doc = None

    del mxd
    return infomatrix_sorted

def zipdir_noroot(path, zipfilename):
    myZipFile = zipfile.ZipFile(os.path.join(scratch,zipfilename),"w")
    for root, dirs, files in os.walk(path):
        for afile in files:
            arcname = os.path.relpath(os.path.join(root, afile), path)
            myZipFile.write(os.path.join(root, afile), arcname)
    myZipFile.close()

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
handler = logging.FileHandler(r"\\cabcvan1gis006\GISData\Aerial_US_Word\python\log\USAerial_US_Word_Log.txt")
handler.setLevel(logging.DEBUG)
logger.addHandler(handler)

# deploy parameter to change
####################################################################################################################
connectionString = 'eris_gis/gis295@cabcvan1ora003.glaciermedia.inc:1521/GMPRODC'
# viewer_path = r'\\cabcvan1eap006\ErisData\Reports\prod\viewer'
viewer_path = r'\\cabcvan1eap003\ErisData\prod\aerial'
reportcheck_path = r'\\cabcvan1eap006\ErisData\Reports\prod\reportcheck\AerialsDigital'
####################################################################################################################
tempyear = '2005'

# global parameters
connectionPath = r"\\cabcvan1gis006\GISData\Aerial_US_Word\python"
mxdfile = r"\\cabcvan1gis006\GISData\Aerial_US_Word\python\templates\template.mxd"
orderGeomlyrfile_point = os.path.join(r"\\cabcvan1gis006\GISData\Aerial_US_Word\python\templates","SiteMaker.lyr")
orderGeomlyrfile_polyline = r"\\cabcvan1gis006\GISData\Aerial_US_Word\python\templates\orderLine.lyr"
orderGeomlyrfile_polygon = r"\\cabcvan1gis006\GISData\Aerial_US_Word\python\templates\orderPoly.lyr"
marginTemplate = r"\\cabcvan1gis006\GISData\Aerial_US_Word\python\templates\margin.docx"
directorytemplate = r'\\cabcvan1gis006\GISData\Aerial_US_Word\python\templates\Environmental-Portrait_AerialOnly'

Summarymxdfile = os.path.join(connectionPath, r"mxd\SummaryPage.mxd")
Covermxdfile = os.path.join(connectionPath, r"mxd\CoverPage.mxd")
coverTemplate = r'\\cabcvan1gis006\GISData\Aerial_US_Word\python\templates\CoverPage'
summaryTemplate = r'\\cabcvan1gis006\GISData\\Aerial_US_Word\python\templates\SummaryPage'

OrderIDText = arcpy.GetParameterAsText(0)
yesBoundary = arcpy.GetParameterAsText(1)
scratch = arcpy.env.scratchWorkspace

# OrderIDText = '1016734'
# OrderNumText = '21020900524'
# yesBoundary = 'y'
# scratch = r"\\cabcvan1gis005\MISC_DataManagement\_AW\AERIAL_WORD_SCRATCHY\%s" %OrderNumText
# if not os.path.exists(scratch):
#     os.mkdir(scratch)

try:
    try:
        con = cx_Oracle.connect(connectionString)
        cur = con.cursor()

        # GET ORDER_ID AND BOUNDARY FROM ORDER_NUM
        if OrderIDText == "":
            cur.execute("SELECT * FROM ERIS.FIM_AUDIT WHERE ORDER_ID IN (select order_id from orders where order_num = '" + str(OrderNumText) + "')")
            result = cur.fetchall()
            OrderIDText = str(result[0][0]).strip()
            # yesBoundaryqry = str([row[3] for row in result if row[2]== "URL"][0])
            # yesBoundary = re.search('(yesBoundary=)(\w+)(&)', yesBoundaryqry).group(2).strip()
            print("Order ID: " + OrderIDText)
            print("Yes Boundary: " + yesBoundary)

        coverInfotext = json.loads(cur.callfunc('eris_gis.getCoverPageInfo', str, (str(OrderIDText),)))
        for key in coverInfotext.keys():
            if coverInfotext[key]=='':
                coverInfotext[key]=' '
        OrderNumText = str(coverInfotext["ORDER_NUM"])
        siteName =coverInfotext["SITE_NAME"]
        proNo = coverInfotext["PROJECT_NUM"]
        ProName = coverInfotext["COMPANY_NAME"]
        siteAddress =coverInfotext["ADDRESS"]
        siteCityState=coverInfotext["CITY"]+", "+coverInfotext["PROVSTATE"]

        coverInfotext["ADDRESS"] = '%s\n%s %s %s'%(coverInfotext["ADDRESS"],coverInfotext["CITY"],coverInfotext["PROVSTATE"],coverInfotext["POSTALZIP"])

##    OrderDetails = json.loads(cur.callfunc('eris_gis.getBufferDetails', str, (str(orderIDText),)))
##    OrderType = OrderDetails["ORDERTYPE"]
##    OrderCoord = eval(OrderDetails["ORDERCOOR"])
##    RadiusType = OrderDetails["RADIUSTYPE"]
        cur.execute("select geometry_type, geometry, radius_type  from eris_order_geometry where order_id =" + OrderIDText)
        t = cur.fetchone()
        OrderType = str(t[0])
        OrderCoord = eval(str(t[1]))
        RadiusType = str(t[2])

        cur.execute("select customer_id from orders where order_id =" + OrderIDText)
        t = cur.fetchone()
        customer_id = str(t[0])
        cur.execute("select address1, address2, city, provstate  from customer where customer_id =" + customer_id)
        t = cur.fetchone()
        if t[1] == None:
            officeAddress = str(t[0])
        else:
            officeAddress = str(t[0])+", "+str(t[1])
        officeCity = str(t[2])+", "+str(t[3])

        cur.execute("select centroid_lat,centroid_long from eris_order_geometry where order_id = " + OrderIDText) # switch to this when there is no explorer
##        cur.execute("select centerlong, centerlat from overlay_image_info where type = 'ae' and order_id = " + OrderIDText) # When an oder does not come with Xplorer, no centroid would be populated to overlay table
        t = cur.fetchone()
        long_center = str(t[0])
        lat_center = str(t[1])

    except Exception,e:
        logger.error("Error to get flag from Oracle " + str(e))
        raise
    finally:
        cur.close()
        con.close()

    docName = OrderNumText+'_US_Aerial.docx'

    arcpy.env.overwriteOutput = True
    arcpy.env.OverWriteOutput = True

    srGCS83 = arcpy.SpatialReference(4269)

    # create the center point shapefile, for positioning
    point = arcpy.Point()
    array = arcpy.Array()
    sr = arcpy.SpatialReference()
    sr.factoryCode = 4269  # requires input geometry is in 4269
    sr.XYTolerance = .00000001
    sr.scaleFactor = 2000
    sr.create()
    featureList = []
    point.X = float(long_center)
    point.Y = float(lat_center)
    sr.setDomain (point.X, point.X, point.Y, point.Y)
    array.add(point)
    feat = arcpy.Multipoint(array, sr)
    # Append to the list of Polygon objects
    featureList.append(feat)

    orderCenter= os.path.join(scratch,"orderCenter.shp")
    arcpy.CopyFeatures_management(featureList, orderCenter)
    arcpy.DefineProjection_management(orderCenter, srGCS83)
    del point, array, feat, featureList

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

    orderGeometry= os.path.join(scratch,"orderGeometry.shp")
    arcpy.CopyFeatures_management(featureList, orderGeometry)
    arcpy.DefineProjection_management(orderGeometry, srGCS83)

    arcpy.AddField_management(orderGeometry, "UTM", "TEXT", "", "", "1500", "", "NULLABLE", "NON_REQUIRED", "")
    arcpy.CalculateUTMZone_cartography(orderGeometry, "UTM")
    UT= arcpy.SearchCursor(orderGeometry)
    for row in UT:
        UTMvalue = str(row.getValue('UTM'))[41:43]
    del UT
    out_coordinate_system = os.path.join(connectionPath+'/', r"projections/NAD1983/NAD1983UTMZone"+UTMvalue+"N.prj")

    orderGeometryPR = os.path.join(scratch, "ordergeoNamePR.shp")
    arcpy.Project_management(orderGeometry, orderGeometryPR, out_coordinate_system)

    del point
    del array

    arcpy.AddField_management(orderGeometryPR, "xCentroid", "DOUBLE", 18, 11)
    arcpy.AddField_management(orderGeometryPR, "yCentroid", "DOUBLE", 18, 11)

    xExpression = "!SHAPE.CENTROID.X!"
    yExpression = "!SHAPE.CENTROID.Y!"

    arcpy.CalculateField_management(orderGeometryPR, "xCentroid", xExpression, "PYTHON_9.3")
    arcpy.CalculateField_management(orderGeometryPR, "yCentroid", yExpression, "PYTHON_9.3")

    in_rows = arcpy.SearchCursor(orderGeometryPR)
    for in_row in in_rows:
        xCentroid = in_row.xCentroid
        yCentroid = in_row.yCentroid
    del in_row
    del in_rows

    worddoclist = []
    #outputPdfname = "map_" + OrderNumText + ".pdf"
    app = win32com.client.DispatchEx("Word.Application")
    app.Visible = 0
    infomatrix_sorted = createWORD(app)

    # step 2:  create summary page
    mxdSummary = arcpy.mapping.MapDocument(Summarymxdfile)

    j=0
    for item in infomatrix_sorted:
        print(item)
        j=j+1
        i=str(j)
        year = item[1]
        source = item[2]
        if item[3] == 6000:
            scale = '1":' + str(item[3]/12)+"'"
        else:
            scale = "1:"+str(item[3])
        comment = item[4]
        exec("e"+i+"1E = arcpy.mapping.ListLayoutElements(mxdSummary, 'TEXT_ELEMENT', 'e"+i+"1')[0]")
        exec("e"+i+"1E.text = year")
        exec("e"+i+"2E = arcpy.mapping.ListLayoutElements(mxdSummary, 'TEXT_ELEMENT', 'e"+i+"2')[0]")
        try:
            exec("e"+i+"2E.text = source")
        except Exception as e:
            print("-----")
            print(traceback.print_exc)
            exec("e"+i+"2E.text = 'USGS'")
        exec("e"+i+"3E = arcpy.mapping.ListLayoutElements(mxdSummary, 'TEXT_ELEMENT', 'e"+i+"3')[0]")
        exec("e"+i+"3E.text = scale")
        if comment <> '':
            exec("e"+i+"4E = arcpy.mapping.ListLayoutElements(mxdSummary, 'TEXT_ELEMENT', 'e"+i+"4')[0]")
            exec("e"+i+"4E.text = comment")

    summaryEmf = os.path.join(scratch, "summary.emf")
    arcpy.mapping.ExportToEMF(mxdSummary, summaryEmf, "PAGE_LAYOUT")
    mxdSummary.saveACopy(os.path.join(scratch, "summarypage.mxd"))
    mxdSummary = None

    zipCover= os.path.join(scratch,'tozip_cover')
    zipSummary = os.path.join(scratch,'tozip_summary')
    if not os.path.exists(zipCover):
        shutil.copytree(coverTemplate,os.path.join(scratch,'tozip_cover'))
    if not os.path.exists(zipSummary):
        shutil.copytree(summaryTemplate,os.path.join(scratch,'tozip_summary'))

    coverPage = goCoverPage(coverInfotext)
    shutil.copyfile(coverPage, os.path.join(zipCover,"word\media\image1.emf"))
    zipdir_noroot(zipCover,"cover.docx")
    shutil.copyfile(summaryEmf, os.path.join(zipSummary,"word\media\image2.emf"))
    zipdir_noroot(zipSummary,"summary.docx")

    # concatenate the word docs into a big final file
    print "#5-0 " + time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
    shutil.copyfile(marginTemplate,os.path.join(scratch,docName))
    finaldoc = app.Documents.Open(os.path.join(scratch,docName))
    sel = finaldoc.ActiveWindow.Selection
    sel.InsertFile(os.path.join(scratch, "summary.docx"))
    sel.InsertBreak()
    npages = 0
    for aDoc in worddoclist:
        npages = npages + 1
        sel.InsertFile(aDoc)
        if npages < len(worddoclist):
           sel.InsertBreak()
    finaldoc.Save()
    finaldoc.Close()

    finaldoc = app.Documents.Open(os.path.join(scratch,docName))
    sel = finaldoc.ActiveWindow.Selection
    sel.InsertFile(os.path.join(scratch,'cover.docx'))
    finaldoc.Save()
    finaldoc.Close()
    finalDoc = None
    app.Application.Quit(-1)
    print "#5-1 " + time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())

    del orderCenter
    del orderGeometry
    shutil.copy(os.path.join(scratch,docName), reportcheck_path)  # occasionally get permission denied issue here when running locally

    arcpy.SetParameterAsText(2, os.path.join(scratch,docName))

    logger.removeHandler(handler)
    handler.close()
except:
    # Get the traceback object
    tb = sys.exc_info()[2]
    tbinfo = traceback.format_tb(tb)[0]
    pymsg = "PYTHON ERRORS:\nTraceback info:\n" + tbinfo + "\nError Info:\n" + str(sys.exc_info()[1])

    try:
        con = cx_Oracle.connect(connectionString)
        cur = con.cursor()
        cur.callproc('eris_aerial.InsertAerialAudit', (OrderIDText, 'python-Error Handling',pymsg))
    finally:
        cur.close()
        con.close()
    raise    # raise the error again

# print ("Final AERIAL report directory: " + (os.path.join(reportcheck_path, docName)))
# print("DONE!")