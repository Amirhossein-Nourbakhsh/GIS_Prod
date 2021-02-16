#-------------------------------------------------------------------------------
# Name:        US TOPO report for Terracon
# Purpose:     create US TOPO report in Terracon required Word format
#
# Author:      jliu
#
# Created:     23/10/2015
# Copyright:   (c) jliu 2015
# Licence:     <your licence>
#-------------------------------------------------------------------------------

# search and create topo maps as usual
# export geometry seperately in emf format
# save picture/vector to the location which links to the word template
# modify text in word template

import time,traceback,json
import sys
import arcpy, os, win32com, cx_Oracle
import csv,glob,urllib
import xml.etree.ElementTree as ET
import operator
import shutil, zipfile
import logging
import ConfigParser
from win32com import client
from time import strftime

print ("#0 " + time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()))

def server_loc_config(configpath,environment):
    configParser = ConfigParser.RawConfigParser()
    configParser.read(configpath)
    if environment == 'test':
        reportcheck = configParser.get('server-config','reportcheck_test')
        reportviewer = configParser.get('server-config','reportviewer_test')
        reportinstant = configParser.get('server-config','instant_test')
        reportnoninstant = configParser.get('server-config','noninstant_test')
        upload_viewer = configParser.get('url-config','uploadviewer')
        server_config = {'reportcheck':reportcheck,'viewer':reportviewer,'instant':reportinstant,'noninstant':reportnoninstant,'viewer_upload':upload_viewer}
        return server_config
    elif environment == 'prod':
        reportcheck = configParser.get('server-config','reportcheck_prod')
        reportviewer = configParser.get('server-config','reportviewer_prod')
        reportinstant = configParser.get('server-config','instant_prod')
        reportnoninstant = configParser.get('server-config','noninstant_prod')
        upload_viewer = configParser.get('url-config','uploadviewer_prod')
        server_config = {'reportcheck':reportcheck,'viewer':reportviewer,'instant':reportinstant,'noninstant':reportnoninstant,'viewer_upload':upload_viewer}
        return server_config
    else:
        return 'invalid server configuration'

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

def dedupMaplist(mapslist):
    if mapslist != []:
    # remove duplicates (same cell and same year)
        if len(mapslist) > 1:   # if just 1, no need to do anything
            mapslist = sorted(mapslist,key=operator.itemgetter(3,0), reverse = True)  # sorted based on year then cell
            i=1
            remlist = []
            while i<len(mapslist):
                row = mapslist[i]
                if row[3] == mapslist[i-1][3] and row[0] == mapslist[i-1][0]:
                    remlist.append(i)
                i = i+1

            for index in sorted(remlist,reverse = True):
                del mapslist[index]
    return mapslist

def countSheets(mapslist):
    if len(mapslist) == 0:
        count = []
    elif len(mapslist) == 1:
        count = [1]
    else:
        count = [1]
        i = 1
        while i < len(mapslist):
            if mapslist[i][3] == mapslist[i-1][3]:
                count.append(count[i-1]+1)
            else:
                count.append(1)
            i = i + 1
    return count

# reorgnize the pdf dictionary based on years
# filter out irrelevant background years (which doesn't have a centre selected map)
def reorgByYear_old(mapslist):    # [64818, 15X15 GRID,  LA_Zachary_335142_1963_62500_geo.pdf,  1963]
    diction_pdf_inPresentationBuffer = {}    #{1975: [geopdf1.pdf, geopdf2.pdf...], 1968: [geopdf11.pdf, ...]}
    diction_pdf_inSearchBuffer = {}
    diction_cellids = {}   # {1975:[cellid1,cellid2...], 1968:[cellid11,cellid12,...]}
    for row in mapslist:
        if row[3] in diction_pdf_inPresentationBuffer.keys():  # {1963:LA_Zachary_335142_1963_62500_geo.pdf, 1975:....}
            diction_pdf_inPresentationBuffer[row[3]].append(row[2])
            diction_cellids[row[3]].append(row[0])
        else:
            diction_pdf_inPresentationBuffer[row[3]] = [row[2]]
            diction_cellids[row[3]] = [row[0]]
    for key in diction_cellids:    # key is the year
        hasSelectedMap = False
        for (cellid,pdfname) in zip(diction_cellids[key],diction_pdf_inPresentationBuffer[key]):
            if cellid in cellids:
                if key in diction_pdf_inSearchBuffer.keys():
                    diction_pdf_inSearchBuffer[key].append(pdfname)
                else:
                    diction_pdf_inSearchBuffer[key] = [pdfname]
                hasSelectedMap = True
                # break;
        if not hasSelectedMap:
            diction_pdf_inPresentationBuffer.pop(key,None)
    return (diction_pdf_inPresentationBuffer,diction_pdf_inSearchBuffer)

def reorgByYear_usingFrame(mapslist):    # [64818, 15X15 GRID,  LA_Zachary_335142_1963_62500_geo.pdf,  1963]
    diction_pdf = {}                     # {1975: [geopdf1.pdf, geopdf2.pdf...], 1968: [geopdf11.pdf, ...]}
    diction_cellids = {}                 # {1975:[cellid1,cellid2...], 1968:[cellid11,cellid12,...]}
    for row in mapslist:
        if row[3] in diction_pdf.keys():  # {1963:LA_Zachary_335142_1963_62500_geo.pdf, 1975:....}
            diction_pdf[row[3]].append(row[2])
            diction_cellids[row[3]].append(row[0])
        else:
            diction_pdf[row[3]] = [row[2]]
            diction_cellids[row[3]] = [row[0]]
    return diction_pdf

def reorgByYear(mapslist):                      # [64818, 15X15 GRID,  LA_Zachary_335142_1963_62500_geo.pdf,  1963]
    diction_pdf_inPresentationBuffer = {}       # {1975: [geopdf1.pdf, geopdf2.pdf...], 1968: [geopdf11.pdf, ...]}
    diction_pdf_inSearchBuffer = {}
    diction_cellids = {}                        # {1975:[cellid1,cellid2...], 1968:[cellid11,cellid12,...]}
    for row in mapslist:
        if row[3] in diction_pdf_inPresentationBuffer.keys():  #{1963:LA_Zachary_335142_1963_62500_geo.pdf, 1975:....}
            diction_pdf_inPresentationBuffer[row[3]].append(row[2])
            diction_cellids[row[3]].append(row[0])
        else:
            diction_pdf_inPresentationBuffer[row[3]] = [row[2]]
            diction_cellids[row[3]] = [row[0]]
    for key in diction_cellids:    # key is the year
        hasSelectedMap = False
        for (cellid,pdfname) in zip(diction_cellids[key],diction_pdf_inPresentationBuffer[key]):
            if cellid in cellids_selected:
                if key in diction_pdf_inSearchBuffer.keys():
                    diction_pdf_inSearchBuffer[key].append(pdfname)
                else:
                    diction_pdf_inSearchBuffer[key] = [pdfname]
                hasSelectedMap = True
                # break;
        if not hasSelectedMap:
            diction_pdf_inPresentationBuffer.pop(key,None)
    return (diction_pdf_inPresentationBuffer,diction_pdf_inSearchBuffer)

# create PDF and also make a copy of the geotiff files if the scale is too small
def createWORD(seriesText,diction, diction_s,app):

    if OrderType.lower()== 'point':
        orderGeomlyrfile = orderGeomlyrfile_point
    elif OrderType.lower() =='polyline':
        orderGeomlyrfile = orderGeomlyrfile_polyline
    else:
        orderGeomlyrfile = orderGeomlyrfile_polygon

    orderGeomLayer = arcpy.mapping.Layer(orderGeomlyrfile)
    orderGeomLayer.replaceDataSource(scratch,"SHAPEFILE_WORKSPACE","orderGeometry")

#    extentLayer = arcpy.mapping.Layer(bufferlyrfile)
#    if seriesText == "7.5":
#        extentLayer.replaceDataSource(scratch,"SHAPEFILE_WORKSPACE","clipFrame_24000")
#    else:   #by default "15"
#        extentLayer.replaceDataSource(scratch,"SHAPEFILE_WORKSPACE","clipFrame_62500")
# arcpy.mapping.AddLayer(df,extentLayer,"Top")

    if not os.path.exists(os.path.join(scratch,'tozip')):
        shutil.copytree(directorytemplate,os.path.join(scratch,'tozip'))

    # add to map template, clip (but need to keep both metadata: year, grid size, quadrangle name(s) and present in order
    mxd = arcpy.mapping.MapDocument(mxdfile)
    df = arcpy.mapping.ListDataFrames(mxd,"*")[0]
    spatialRef = arcpy.SpatialReference(out_coordinate_system)
    df.spatialReference = spatialRef
    if yesBoundary.lower()=='y' or yesBoundary.lower() == 'yes':
        arcpy.mapping.AddLayer(df,orderGeomLayer,"Top")

    years = diction.keys()
    years.sort(reverse = True)
    for year in years:
        print ("#0 " + time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()) + "    " + str(year))
        if int(year) < 2008:
            tifdir = tifdir_h
            if len(years) > 1:
                topofile = topolyrfile_b
            else:
                topofile = topolyrfile_none
            mscale = int(diction[year][0].split('_')[-2])   # assumption: WI_Ashland East_500066_1964_24000_geo.pdf, and all pdfs from the same year are of the same scale

        else:
            tifdir = tifdir_c
            if len(years) > 1:
                topofile = topolyrfile_w
            else:
                topofile = topolyrfile_none
            mscale = 24000
        print ("########" + str(mscale))

        for lyr in arcpy.mapping.ListLayers(mxd, "", df):
            if lyr.name == "Project Property":
                if OrderType.lower() == "point":
                    lyr.visible = False
                else:
                    lyr.visible = True
                df.extent = lyr.getSelectedExtent(False)

        # if seriesText == "7.5":
        #     df.scale = 24000
        # else:
        #     df.scale = 62500

        # use uniform presentation scale for all maps
        df.scale = 24000
        # df.scale = 6000
        arcpy.RefreshTOC()
        arcpy.RefreshActiveView()

        outputemf = os.path.join(scratch, "map_"+seriesText+"_"+year+".emf")
        arcpy.mapping.ExportToEMF(mxd, outputemf, "PAGE_LAYOUT")
        mxd.saveACopy(os.path.join(scratch,seriesText+"_"+year+"_emf.mxd"))

        copydir = os.path.join(scratch,deliverfolder,str(year)+"_"+seriesText+"_"+str(mscale))
        os.makedirs(copydir)    # WI_Marengo_503367_1984_24000_geo.pdf -> 1984_7.5_24000

        pdfnames = diction[year]
        pdfnames.sort()

        quadrangles = ""
        seq = 0
        for pdfname in pdfnames:

            tifname = pdfname[0:-4]     # note without .tif part
            tifname_bk = tifname
            if os.path.exists(os.path.join(tifdir,tifname+ "_t.tif")):
                if '.' in tifname:
                    tifname = tifname.replace('.','')

                # need to make a local copy of the tif file for fast data source replacement
                namecomps = tifname.split('_')
                namecomps.insert(-2,year)
                newtifname = '_'.join(namecomps)

                shutil.copyfile(os.path.join(tifdir,tifname_bk+"_t.tif"),os.path.join(copydir,newtifname+'.tif'))
                logger.debug(os.path.join(tifdir,tifname+"_t.tif"))
                topoLayer = arcpy.mapping.Layer(topofile)
                topoLayer.replaceDataSource(copydir, "RASTER_WORKSPACE", newtifname)
                topoLayer.name = newtifname
                arcpy.mapping.AddLayer(df, topoLayer, "BOTTOM")

                if pdfname in diction_s[year]:
                    comps = diction[year][seq].split('_')
                    if int(year)<2008:
                        quadname = comps[1] +","+comps[0]
                    else:
                        quadname = " ".join(comps[1:len(comps)-3])+","+comps[0]


                    if quadrangles =="":
                        quadrangles = quadname
                    else:
                        quadrangles = quadrangles + "; " + quadname

            else:
                print ("tif file doesn't exist " + tifname)
                logger.debug("tif file doesn't exist " + tifname)
                if not os.path.exists(tifdir):
                    logger.debug("tif dir doesn't exist " + tifdir)
                else:
                    logger.debug("tif dir does exist " + tifdir)
            seq = seq + 1

        # df.extent = extentLayer.getSelectedExtent(False) # this helps centre the map
        # turn off the project property layer
        # for lyr in arcpy.mapping.ListLayers(mxd, "", df):
        #     if lyr.name == "Project Property":
        #         lyr.visible = True

        # arcpy.mapping.RemoveLayer(df,extentBufferLayer) # this is not working
        arcpy.RefreshTOC()
        arcpy.RefreshActiveView()
        outputjpg = os.path.join(scratch, "map_"+seriesText+"_"+year+".jpg")
        print (seriesText + '_'+year)
        if int(year)<2008:
            arcpy.mapping.ExportToJPEG(mxd, outputjpg, "PAGE_LAYOUT", 480, 640, 125, "False", "24-BIT_TRUE_COLOR", 90)    #note: because of exporting PAGE_LAYOUT, the wideth and height parameters are ignored.
        else:
            arcpy.mapping.ExportToJPEG(mxd, outputjpg, "PAGE_LAYOUT", 480, 640, 250, "False", "24-BIT_TRUE_COLOR", 90)

        mxd.saveACopy(os.path.join(scratch,seriesText+"_"+year+".mxd"))

        # remove all the raster layers
        for lyr in arcpy.mapping.ListLayers(mxd, "", df):
            if lyr.name != "Project Property":
                arcpy.mapping.RemoveLayer(df, lyr)      # remove the clipFrame layer

        # copy over the unzipped directory
        # change the image files
        # zip up to .docx and change text
        # shutil.copyfile(terraconTemplate,os.path.join(scratch,seriesText+"_"+year+".docx"))

        shutil.copyfile(os.path.join(scratch,"map_"+seriesText+"_"+year+".jpg"), os.path.join(scratch,"tozip\word\media\image2.jpg"))
        shutil.copyfile(os.path.join(scratch,"map_"+seriesText+"_"+year+".emf"), os.path.join(scratch,"tozip\word\media\image1.emf"))
        zipdir_noroot(os.path.join(scratch,'tozip'),seriesText+"_"+year+".docx")
        worddoclist.append(os.path.join(scratch,seriesText+"_"+year+".docx"))

        # the word template has been copied, the image files have also been copied, need to refresh and replace the text fields, save
        doc = app.Documents.Open(os.path.join(scratch,seriesText+"_"+year+".docx"))

        fileName = OrderNumText
        fileDate = time.strftime('%Y-%m-%d', time.localtime())
        print ("quandrangles are " + quadrangles)
        quads = 'TOPOGRAPHIC MAP IMAGE COURTESY OF THE U.S. GEOLOGICAL SURVEY\rQUADRANGLES INCLUDE: ' + quadrangles + ' (' + seriesText +' Min Series, '+ str(year) + ').'

        allShapes = doc.Shapes
        allShapes(3).TextFrame.TextRange.Text = 'TOPOGRAPHIC MAP (' + str(year) + ')'    #TOPOGRAPHIC MAP title line
        txt = allShapes(4).TextFrame.TextRange.Text.replace('Site Name', siteName)
        # allShapes(4).TextFrame.TextRange.Text.replace('Site Address', siteAddress)
        # allShapes(4).TextFrame.TextRange.Text.replace('Site City, Site State', siteCityState)
        txt = txt.replace('Site Address', siteAddress)
        txt = txt.replace('Site City, Site State', siteCityState)
        if not custom_profile:
            allShapes(4).TextFrame.TextRange.Text = txt
            allShapes(9).TextFrame.TextRange.Text = quads
            allShapes(11).TextFrame.TextRange.Text = officeAddress
            allShapes(12).TextFrame.TextRange.Text = officeCity
            allShapes(13).TextFrame.TextRange.Text = proNo
            allShapes(24).TextFrame.TextRange.Text = fileName
            allShapes(25).TextFrame.TextRange.Text = fileDate
        else:
            allShapes(4).TextFrame.TextRange.Text = txt
            allShapes(9).TextFrame.TextRange.Text = quads
            allShapes(11).TextFrame.TextRange.Text = officeAddress
            allShapes(12).TextFrame.TextRange.Text = officeCity
            allShapes(13).TextFrame.TextRange.Text = proNo
            allShapes(26).TextFrame.TextRange.Text = fileName
            allShapes(27).TextFrame.TextRange.Text = fileDate
        # allShapes(27).TextFrame.TextRange.Text = fileName
        # allShapes(28).TextFrame.TextRange.Text = fileDate
        doc.Save()
        doc.Close()
        doc = None

    del mxd
    return "Success! :)"

def zipdir_noroot(path, zipfilename):
    myZipFile = zipfile.ZipFile(os.path.join(scratch,zipfilename),"w")
    for root, dirs, files in os.walk(path):
        for afile in files:
            arcname = os.path.relpath(os.path.join(root, afile), path)
            myZipFile.write(os.path.join(root, afile), arcname)
    myZipFile.close()

# ===================================================================================================
# deploy parameter to change
# ---------------------------------------------------------------------------------------------------
server_environment = 'prod'
server_config_file = r"\\cabcvan1gis007\gptools\ERISServerConfig.ini"
server_config = server_loc_config(server_config_file,server_environment)

connectionString = 'eris_gis/gis295@cabcvan1ora003.glaciermedia.inc:1521/GMPRODC'
viewer_path = server_config["viewer"]
upload_link = server_config["viewer_upload"] + r"/ErisInt/BIPublisherPortal_prod/Viewer.svc/"
# test: upload_link = r"http://CABCVAN1OBI002/ErisInt/BIPublisherPortal/Viewer.svc/"
# production: upload_link = r"http://CABCVAN1OBI002/ErisInt/BIPublisherPortal_prod/Viewer.svc/"
reportcheckFolder = server_config["reportcheck"]
# ---------------------------------------------------------------------------------------------------
custom_profile = False

connectionPath = r'\\cabcvan1gis006\GISData\Topo_US_Word'
masterlyr = os.path.join(connectionPath,"masterfile\Cell_PolygonAll.shp")
csvfile_h = os.path.join(connectionPath,"masterfile\All_HTMC_all_all_gda_results.csv")
csvfile_c = os.path.join(connectionPath,"masterfile\All_USTopo_T_7.5_gda_results.csv")
tifdir_h = r'\\cabcvan1fpr009\USGS_Topo\USGS_HTMC_Geotiff'
tifdir_c = r'\\cabcvan1fpr009\USGS_Topo\USGS_currentTopo_Geotiff'
mxdfile = os.path.join(connectionPath,r"python\templates\template.mxd")
topolyrfile_none = os.path.join(connectionPath,r"python\templates\topo.lyr")
topolyrfile_b = os.path.join(connectionPath,r"python\templates\topo_black.lyr")
topolyrfile_w = os.path.join(connectionPath,r"python\templates\topo_white.lyr")
bufferlyrfile = os.path.join(connectionPath,r"python\templates\buffer_extent.lyr")
orderGeomlyrfile_point = os.path.join(connectionPath,r"python\templates\SiteMaker.lyr")
orderGeomlyrfile_polyline = os.path.join(connectionPath,r"python\templates\orderLine.lyr")
orderGeomlyrfile_polygon = os.path.join(connectionPath,r"python\templates\orderPoly.lyr")
readmefile = os.path.join(connectionPath,r"python\templates\readme.txt")
terraconTemplate = os.path.join(connectionPath,r"python\templates\Environmental-Portrait_TopoOnly_noarrow.docx")
marginTemplate = os.path.join(connectionPath,r"python\templates\margin.docx")
directorytemplate =os.path.join(connectionPath,r"python\templates\Environmental-Portrait_TopoOnly_noarrow")
Summarymxdfile = os.path.join(connectionPath, r"python\mxd\SummaryPage.mxd")
Covermxdfile = os.path.join(connectionPath, r"python\mxd\CoverPage.mxd")
coverTemplate = os.path.join(connectionPath,r"python\templates\CoverPage")
summaryTemplate = os.path.join(connectionPath,r"python\templates\SummaryPage")

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
handler = logging.FileHandler(os.path.join(u'\\\\cabcvan1gis006\\GISData\\Topo_US_Word',r"python\log\USTopoSearch_Terracon_Log.txt"))
handler.setLevel(logging.DEBUG)
logger.addHandler(handler)

lookup_state = {
'AL': 'Alabama',
'AK': 'Alaska',
'AZ': 'Arizona',
'AR': 'Arkansas',
'CA': 'California',
'CO': 'Colorado',
'CT': 'Connecticut',
'DC': 'District of Columbia',
'DE': 'Delaware',
'FL': 'Florida',
'GA': 'Georgia',
'HI': 'Hawaii',
'ID': 'Idaho',
'IL': 'Illinois',
'IN': 'Indiana',
'IA': 'Iowa',
'KS': 'Kansas',
'KY': 'Kentucky',
'LA': 'Louisiana',
'ME': 'Maine',
'MD': 'Maryland',
'MA': 'Massachusetts',
'MI': 'Michigan',
'MN': 'Minnesota',
'MS': 'Mississippi',
'MO': 'Missouri',
'MT': 'Montana',
'NE': 'Nebraska',
'NV': 'Nevada',
'NH': 'New Hampshire',
'NJ': 'New Jersey',
'NM': 'New Mexico',
'NY': 'New York',
'NC': 'North Carolina',
'ND': 'North Dakota',
'OH': 'Ohio',
'OK': 'Oklahoma',
'OR': 'Oregon',
'PA': 'Pennsylvania',
'RI': 'Rhode Island',
'SC': 'South Carolina',
'SD': 'South Dakota',
'TN': 'Tennessee',
'TX': 'Texas',
'UT': 'Utah',
'VT': 'Vermont',
'VA': 'Virginia',
'WA': 'Washington',
'WV': 'West Virginia',
'WI': 'Wisconsin',
'WY': 'Wyoming',
'PR': 'Puerto Rico',
'VI': 'Virgin Islands',
'ON': 'Ontario',
'BC': 'British Columbia',
'AB': 'Alberta',
'MB': 'Manitoba',
'SK': 'Saskatchewan',
'QC': 'Quebec',
'NS': 'Nova Scotia',
'NB': 'New Brunswick',
'PE': 'Prince Edward Island',
'NL': 'Newfoundland and Labrador',
'NT': 'Northwest Territories',
'YK': 'Yukon',
'NU': 'Nunavut'
}

try:
    # OrderIDText = arcpy.GetParameterAsText(0)
    # yesBoundary = arcpy.GetParameterAsText(1)
    # scratch = arcpy.env.scratchWorkspace
    OrderIDText = ""
    OrderNumText = r"20100100012"
    yesBoundary = 'yes'
    scratch = os.path.join(r"W:\Data Analysts\Alison\_GIS\WORD_SCRATCHY", OrderNumText)
# ===================================================================================================

    if yesBoundary == 'arrow':
        yesBoundary = 'yes'
        custom_profile = True
    else:
        print ('no custom profile set')

    if not custom_profile:
        terraconTemplate = os.path.join(connectionPath,r"python\templates\Environmental-Portrait_TopoOnly_noarrow_fin.docx")
        directorytemplate =os.path.join(connectionPath,r"python\templates\Environmental-Portrait_TopoOnly_noarrow_fin")
    else:
        terraconTemplate = os.path.join(connectionPath,r"python\templates\Environmental-Portrait_TopoOnly_dev_fin.docx")
        directorytemplate =os.path.join(connectionPath,r"python\templates\Environmental-Portrait_TopoOnly_dev_fin")

    try:
#        con = cx_Oracle.connect(connectionString)
#        cur = con.cursor()

#        coverInfotext = json.loads(cur.callfunc('eris_gis.getCoverPageInfo', str, (str(OrderIDText),)))
#        for key in coverInfotext.keys():
#            if coverInfotext[key]=='':
#                coverInfotext[key]=' '
#        OrderNumText = str(coverInfotext["ORDER_NUM"])
#        siteName =coverInfotext["SITE_NAME"]
#        proNo = coverInfotext["PROJECT_NUM"]
#        ProName = coverInfotext["COMPANY_NAME"]
#        siteAddress =coverInfotext["ADDRESS"]

#        siteCityState=coverInfotext["CITY"]+", "+coverInfotext["PROVSTATE"]
#        coverInfotext["ADDRESS"] = '%s\n%s %s %s'%(coverInfotext["ADDRESS"],coverInfotext["CITY"],coverInfotext["PROVSTATE"],coverInfotext["POSTALZIP"])

#        cur.execute("select customer_id from orders where order_id =" + OrderIDText)
#        t = cur.fetchone()
#        customer_id = str(t[0])

#        OrderDetails = json.loads(cur.callfunc('eris_gis.getBufferDetails', str, (str(OrderIDText),)))
#        OrderType = OrderDetails["ORDERTYPE"]
#        OrderCoord = eval(OrderDetails["ORDERCOOR"])
#        RadiusType = OrderDetails["RADIUSTYPE"]

#        cur.execute("select address1, address2, city, provstate  from customer where customer_id =" + customer_id)
#        t = cur.fetchone()
#        if t[1] == None:
#            officeAddress = str(t[0])
#        else:
#            officeAddress = str(t[0])+", "+str(t[1])
#        officeCity = str(t[2])+", "+str(t[3])
        con = cx_Oracle.connect(connectionString)
        cur = con.cursor()

        # GET ORDER_ID AND BOUNDARY FROM ORDER_NUM
        if OrderIDText == "":
            cur.execute("SELECT * FROM ERIS.FIM_AUDIT WHERE ORDER_ID IN (select order_id from orders where order_num = '" + str(OrderNumText) + "')")
            result = cur.fetchall()
            OrderIDText = str(result[0][0]).strip()
            print("Order ID: " + OrderIDText)

        coverInfotext = json.loads(cur.callfunc('eris_gis.getCoverPageInfo', str, (str(OrderIDText),)))
        for key in coverInfotext.keys():
            if coverInfotext[key]=='':
                coverInfotext[key]=' '
        OrderNumText = str(coverInfotext["ORDER_NUM"])
        siteName =coverInfotext["SITE_NAME"]
        proNo = coverInfotext["PROJECT_NUM"]
        ProName = coverInfotext["COMPANY_NAME"]
        siteAddress =coverInfotext["ADDRESS"]
        coverState = lookup_state[coverInfotext["PROVSTATE"]]+" "+coverInfotext["POSTALZIP"]
        siteCityState=coverInfotext["CITY"]+", "+coverState

        coverInfotext["ADDRESS"] = '%s\n%s %s %s'%(coverInfotext["ADDRESS"],coverInfotext["CITY"],coverInfotext["PROVSTATE"],coverInfotext["POSTALZIP"])

        cur.execute("select customer_id from orders where order_id =" + OrderIDText)
        t = cur.fetchone()
        customer_id = str(t[0])

        # OrderDetails = json.loads(cur.callfunc('eris_gis.getBufferDetails', str, (str(OrderIDText),)))
        # OrderType = OrderDetails["ORDERTYPE"]
        # OrderCoord = eval(OrderDetails["ORDERCOOR"])
        # RadiusType = OrderDetails["RADIUSTYPE"]

        cur.execute("select geometry_type, geometry, radius_type  from eris_order_geometry where order_id =" + OrderIDText)
        t = cur.fetchone()
        OrderType = str(t[0])
        OrderCoord = eval(str(t[1]))
        RadiusType = str(t[2])

        cur.execute("select address1, address2, city, provstate, postal_code  from customer where customer_id =" + customer_id)
        t = cur.fetchone()
        if t[1] == None:
            officeAddress = str(t[0])
        else:
            officeAddress = str(t[0])+", "+str(t[1])
        officeCity = str(t[2])+", "+lookup_state[str(t[3])]+" "+str(t[4])

    except Exception as e:
        logger.error("Error to get flag from Oracle " + str(e))
        raise
    finally:
        cur.close()
        con.close()

    deliverfolder = OrderNumText
    docreport = OrderNumText+"_US_Topo.docx"

    arcpy.env.overwriteOutput = True
    arcpy.env.OverWriteOutput = True

    srGCS83 = arcpy.SpatialReference(os.path.join(connectionPath, r"python\projections\GCSNorthAmerican1983.prj"))

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
    arcpy.CalculateUTMZone_cartography(orderGeometry, 'UTM')
    UT= arcpy.SearchCursor(orderGeometry)
    for row in UT:
        UTMvalue = str(row.getValue('UTM'))[41:43]
    del UT
    out_coordinate_system = os.path.join(connectionPath, r"python\projections\NAD1983\NAD1983UTMZone"+UTMvalue+"N.prj")

    orderGeometryPR = os.path.join(scratch, "ordergeoNamePR.shp")
    arcpy.Project_management(orderGeometry, orderGeometryPR, out_coordinate_system)

    del point
    del array

    arcpy.AddField_management(orderGeometryPR, "xCentroid", "DOUBLE", 18, 11)
    arcpy.AddField_management(orderGeometryPR, "yCentroid", "DOUBLE", 18, 11)

    xExpression = '!SHAPE.CENTROID.X!'
    yExpression = '!SHAPE.CENTROID.Y!'

    arcpy.CalculateField_management(orderGeometryPR, 'xCentroid', xExpression, "PYTHON_9.3")
    arcpy.CalculateField_management(orderGeometryPR, 'yCentroid', yExpression, "PYTHON_9.3")

    in_rows = arcpy.SearchCursor(orderGeometryPR)
    for in_row in in_rows:
        xCentroid = in_row.xCentroid
        yCentroid = in_row.yCentroid
    del in_row
    del in_rows

    logger.debug("#1")
    masterLayer = arcpy.mapping.Layer(masterlyr)
    # arcpy.SelectLayerByLocation_management(masterLayer,'intersect', clipFrame_24000)
    arcpy.SelectLayerByLocation_management(masterLayer,'intersect', orderGeometryPR,'0.25 KILOMETERS')  #it doesn't seem to work without the distance

    logger.debug("#2")
    if(int((arcpy.GetCount_management(masterLayer).getOutput(0))) ==0):

        print ("NO records selected")
        masterLayer = None

    else:
        cellids_selected = []
        # loop through the relevant records, locate the selected cell IDs
        rows = arcpy.SearchCursor(masterLayer)    # loop through the selected records
        for row in rows:
            cellid = str(int(row.getValue("CELL_ID")))
            cellids_selected.append(cellid)
        del row
        del rows

        arcpy.SelectLayerByLocation_management(masterLayer,'intersect', orderGeometryPR,'10 KILOMETERS','NEW_SELECTION')
        cellids = []
        cellsizes = []
        # loop through the relevant records, locate the selected cell IDs
        rows = arcpy.SearchCursor(masterLayer)    # loop through the selected records
        for row in rows:
            cellid = str(int(row.getValue("CELL_ID")))
            cellsize = str(int(row.getValue("CELL_SIZE")))
            cellids.append(cellid)
            cellsizes.append(cellsize)
        del row
        del rows
        masterLayer = None
        logger.debug(cellids)

        infomatrix = []
        # cellids are found, need to find corresponding map .pdf by reading the .csv file
        # also get the year info from the corresponding .xml
        print ("#1 " + time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()))
        with open(csvfile_h, "rb") as f:
            reader = csv.reader(f)
            for row in reader:
                if row[9] in cellids:
                    # print "#2 " + time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
                    pdfname = row[15].strip()

                    #read the year from .xml file
                    xmlname = pdfname[0:-3] + "xml"
                    xmlpath = os.path.join(tifdir_h,xmlname)
                    tree = ET.parse(xmlpath)
                    root = tree.getroot()
                    procsteps = root.findall("./dataqual/lineage/procstep")
                    yeardict = {}
                    for procstep in procsteps:
                        procdate = procstep.find("./procdate")
                        if procdate != None:
                            procdesc = procstep.find("./procdesc")
                            yeardict[procdesc.text.lower()] = procdate.text
                    # print yeardict
                    year2use = ""
                    yearcandidates = []
                    if "edit year" in yeardict.keys():
                        yearcandidates.append(int(yeardict["edit year"]))

                    if "aerial photo year" in yeardict.keys():
                        yearcandidates.append(int(yeardict["aerial photo year"]))

                    if "photo revision year" in yeardict.keys():
                        yearcandidates.append(int(yeardict["photo revision year"]))

                    if "field check year" in yeardict.keys():
                        yearcandidates.append(int(yeardict["field check year"]))

                    if "photo inspection year" in yeardict.keys():
                        # print "photo inspection year is " + yeardict["photo inspection year"]
                        yearcandidates.append(int(yeardict["photo inspection year"]))

                    if "date on map" in yeardict.keys():
                        # print "date on  map " + yeardict["date on map"]
                        yearcandidates.append(int(yeardict["date on map"]))


                    if len(yearcandidates) > 0:
                        # print "***** length of yearcnadidates is " + str(len(yearcandidates))
                        year2use = str(max(yearcandidates))

                    if year2use == "":
                        print ("################### cannot determine the year of the map!!")

                    # logger.debug(row[9] + " " + row[5] + "  " + row[15] + "  " + year2use)
                    infomatrix.append([row[9],row[5],row[15],year2use])  # [64818, 15X15 GRID,  LA_Zachary_335142_1963_62500_geo.pdf,  1963]

        with open(csvfile_c, "rb") as f:
            reader = csv.reader(f)
            for row in reader:
                if row[9] in cellids:
                    # print "#2 " + time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
                    pdfname = row[15].strip()

                    # for current topos, read the year from the geopdf file name
                    templist = pdfname.split("_")
                    year2use = templist[len(templist)-3][0:4]

                    if year2use[0:2] != "20":
                        print ("################### Error in the year of the map!!")

                    print (row[9] + " " + row[5] + "  " + row[15] + "  " + year2use)
                    infomatrix.append([row[9],row[5],row[15],year2use])

        logger.debug("#3")
        print ("#3 " + time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()))
        # locate the geopdf and find the exact year to report, only use one from the same year per cell
        maps7575 = []
        maps1515 = []
        maps3060 =[]
        maps12 = []

        for row in infomatrix:
            if row[1] == "7.5X7.5 GRID":
                maps7575.append(row)
            elif row[1] == "15X15 GRID":
                maps1515.append(row)
            elif row[1] == "30X60 GRID":
                maps3060.append(row)
            elif row[1] == "1X2 GRID":
                maps12.append(row)

        # dedup the duplicated years
        maps7575 = dedupMaplist(maps7575)
        maps1515 = dedupMaplist(maps1515)
        maps3060 = dedupMaplist(maps3060)
        maps12 = dedupMaplist(maps12)

        logger.debug("#4")
        print ("#4 " + time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()))

        # reorganize data structure
        (dict7575,dict7575_s) = reorgByYear(maps7575)  #{1975: geopdf.pdf, 1973: ...}
        (dict1515,dict1515_s) = reorgByYear(maps1515)

    # step 2:  create summary page
        mxdSummary = arcpy.mapping.MapDocument(Summarymxdfile)

        # update summary table
        years75 = dict7575.keys()
        years75.sort(reverse=True)
        years15 = dict1515.keys()
        years15.sort(reverse=True)

        j = 0
        for year in years75:
            j=j+1
            i = str(j)
            mapseries = '7.5'
            exec("e"+i+"1E = arcpy.mapping.ListLayoutElements(mxdSummary, 'TEXT_ELEMENT', 'e"+i+"1')[0]")
            exec("e"+i+"1E.text = year")
            exec("e"+i+"2E = arcpy.mapping.ListLayoutElements(mxdSummary, 'TEXT_ELEMENT', 'e"+i+"2')[0]")
            exec("e"+i+"2E.text = mapseries")

        for year in years15:
            j=j+1
            i=str(j)
            mapseries = '15'
            exec("e"+i+"1E = arcpy.mapping.ListLayoutElements(mxdSummary, 'TEXT_ELEMENT', 'e"+i+"1')[0]")
            exec("e"+i+"1E.text = year")
            exec("e"+i+"2E = arcpy.mapping.ListLayoutElements(mxdSummary, 'TEXT_ELEMENT', 'e"+i+"2')[0]")
            exec("e"+i+"2E.text = mapseries")

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
        shutil.copyfile(coverPage, os.path.join(zipCover,"word\media\image2.emf"))
        zipdir_noroot(zipCover,"cover.docx")
        shutil.copyfile(summaryEmf, os.path.join(zipSummary,"word\media\image2.emf"))
        zipdir_noroot(zipSummary,"summary.docx")

    # step 3: create the actual word pages
        logger.debug("#5")
        worddoclist = []
        # outputPdfname = "map_" + OrderNumText + ".pdf"
        app = win32com.client.DispatchEx("Word.Application")
        app.Visible = 0
        createWORD("7.5",dict7575,dict7575_s,app)
        createWORD("15",dict1515,dict1515_s,app)

        print ("#5-0 " + time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()))
        # concatenate the word docs into a big final file

        shutil.copyfile(marginTemplate,os.path.join(scratch,docreport))
        finaldoc = app.Documents.Open(os.path.join(scratch,docreport))
        sel = finaldoc.ActiveWindow.Selection
        npages = 0
        sel.InsertFile(os.path.join(scratch,'cover.docx'))
        sel.InsertBreak()
        sel.InsertFile(os.path.join(scratch,'summary.docx'))
        sel.InsertBreak()
        for aDoc in worddoclist:
            npages = npages + 1
            sel.InsertFile(aDoc)
            if npages < len(worddoclist):
               sel.InsertBreak()
        # finaldoc.SaveAs(os.path.join(scratch,"test.docx"))
        finaldoc.Save()
        finaldoc.Close()
        finalDoc = None
        app.Application.Quit()

        print ("#5-1 " + time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()))

        needViewer = 'N'
        # check if need to copy data for Topo viewer
        try:
            con = cx_Oracle.connect(connectionString)
            cur = con.cursor()

            cur.execute("select topo_viewer from order_viewer where order_id =" + str(OrderIDText))
            t = cur.fetchone()
            if t != None:
                needViewer = t[0]

        finally:
            cur.close()
            con.close()

        if needViewer == 'Y':
            srGoogle = arcpy.SpatialReference(3857)   #web mercator
            srWGS84 = arcpy.SpatialReference(4326)   #WGS84
            metadata = []

            srGoogle = arcpy.SpatialReference(3857)   #web mercator
            arcpy.AddMessage("Viewer is needed. Need to copy data to obi002")
            viewerdir = os.path.join(scratch,OrderNumText+'_topo')
            if not os.path.exists(viewerdir):
                os.mkdir(viewerdir)
            tempdir = os.path.join(scratch,'viewertemp')
            if not os.path.exists(tempdir):
                os.mkdir(tempdir)
            # need to reorganize deliver directory

            dirs = filter(os.path.isdir, glob.glob(os.path.join(scratch,deliverfolder)+'\*_7.5_*'))
            if len(dirs) > 0:
                if not os.path.exists(os.path.join(viewerdir,"75")):
                    os.mkdir(os.path.join(viewerdir,"75"))
                # get the extent to use. use one uniform for now
                year = dirs[0].split('_7.5_')[0][-4:]
                mxdname = '7.5_'+year+'.mxd'
                mxd = arcpy.mapping.MapDocument(os.path.join(scratch,mxdname))
                df = arcpy.mapping.ListDataFrames(mxd,"*")[0]    # the spatial reference here is UTM zone #, need to change to WGS84 Web Mercator
                df.spatialReference = srGoogle
                extent = df.extent

                del df, mxd
                XMAX = extent.XMax
                XMIN = extent.XMin
                YMAX = extent.YMax
                YMIN = extent.YMin
                pnt1 = arcpy.Point(XMIN, YMIN)
                pnt2 = arcpy.Point(XMIN, YMAX)
                pnt3 = arcpy.Point(XMAX, YMAX)
                pnt4 = arcpy.Point(XMAX, YMIN)
                array = arcpy.Array()
                array.add(pnt1)
                array.add(pnt2)
                array.add(pnt3)
                array.add(pnt4)
                array.add(pnt1)
                polygon = arcpy.Polygon(array)
                arcpy.CopyFeatures_management(polygon, os.path.join(tempdir, "Extent75.shp"))
                arcpy.DefineProjection_management(os.path.join(tempdir, "Extent75.shp"), srGoogle)

                arcpy.Project_management(os.path.join(tempdir, "Extent75.shp"), os.path.join(tempdir,"Extent75_WGS84.shp"), srWGS84)
                desc = arcpy.Describe(os.path.join(tempdir, "Extent75_WGS84.shp"))
                lat_sw = desc.extent.YMin
                long_sw = desc.extent.XMin
                lat_ne = desc.extent.YMax
                long_ne = desc.extent.XMax
                # clip_envelope = str(extent75.XMin) + " " + str(extent75.YMin) + " " + str(extent75.XMax) + " " + str(extent75.YMax)
            # arcpy.env.compression = "JPEG 85"

            arcpy.env.outputCoordinateSystem = srGoogle
            # arcpy.env.extent = extent
            for dir in dirs:
                metaitem = {}

                year = dir.split('_7.5_')[0][-4:]
                # clip and then mosaick the tifs to a jpg
                # arcpy.env.workspace = tempdir
                n = 0
                jpgdir = os.path.join(tempdir,"75_"+year)
                if not os.path.exists(jpgdir):
                    os.mkdir(jpgdir)

                for tif in filter(os.path.isfile, glob.glob(dir+'\*.tif')):
                    try:
                        arcpy.Clip_management(tif,"",os.path.join(jpgdir, "tempClip"+str(n)+".jpg"),os.path.join(tempdir, "Extent75.shp"),"255","ClippingGeometry")   #this will create proper transparency for no data area
                        # arcpy.Clip_management(tif,"",os.path.join(jpgdir, "tempClip"+str(n)+".jpg"),os.path.join(tempdir, "Extent75.shp"),"","ClippingGeometry")
                        n = n + 1
                    except Exception as e:
                        print (str(e) + tif)
                print ("year " + str(year) + ", n = " + str(n))
                print ("images to mosaick: ")
                print (filter(os.path.isfile,glob.glob(jpgdir+"\*.jpg")))
                arcpy.MosaicToNewRaster_management(filter(os.path.isfile,glob.glob(jpgdir+"\*.jpg")), os.path.join(viewerdir,"75"),year+".jpg",srGoogle,"","","3","MINIMUM","MATCH")
                arcpy.MosaicToNewRaster_management(filter(os.path.isfile,glob.glob(jpgdir+"\*.jpg")), os.path.join(viewerdir,"75"),year+"-84.jpg",srWGS84,"","","3","MINIMUM","MATCH")
                metaitem['type'] = 'topo75'
                metaitem['imagename'] = year+'.jpg'
                desc = arcpy.Describe(os.path.join(viewerdir, "75", year+'-84.jpg'))
                metaitem['lat_sw'] = desc.extent.YMin
                metaitem['long_sw'] = desc.extent.XMin
                metaitem['lat_ne'] = desc.extent.YMax
                metaitem['long_ne'] = desc.extent.XMax

                os.remove(os.path.join(viewerdir,"75",year+"-84.jpg"))
                metadata.append(metaitem)
            arcpy.env.outputCoordinateSystem = None
            # arcpy.env.extent = None

            dirs = filter(os.path.isdir, glob.glob(os.path.join(scratch,deliverfolder)+'\*_15_*'))
            if len(dirs) > 0:
                if not os.path.exists(os.path.join(viewerdir,"150")):
                    os.mkdir(os.path.join(viewerdir,"150"))
                # get the extent to use. use one uniform for now
            # arcpy.env.compression = "JPEG 85"
            arcpy.env.outputCoordinateSystem = srGoogle
            # arcpy.env.extent = extent
            for dir in dirs:
                metaitem  ={}
                year = dir.split('_15_')[0][-4:]
                # clip and then mosaick the tifs to a jpg
                # arcpy.env.workspace = os.path.join(os.path.join(viewerdir,"150"))
                # arcpy.env.workspace = tempdir
                jpgdir = os.path.join(tempdir,"150_"+year)
                if not os.path.exists(jpgdir):
                    os.mkdir(jpgdir)

                n = 0
                for tif in filter(os.path.isfile, glob.glob(dir+'\*.tif')):
                    try:
                        # note; use the same Extent75 for clipping
                        arcpy.Clip_management(tif,"",os.path.join(jpgdir,"tempClip"+str(n)+".jpg"),os.path.join(tempdir, "Extent75.shp"),"255","ClippingGeometry")    # this will produce proper transparency for no data area
                        # arcpy.Clip_management(tif,"",os.path.join(jpgdir,"tempClip"+str(n)+".jpg"),os.path.join(tempdir, "Extent75.shp"),"","ClippingGeometry")    # this doesn't fill outside nodata area, and this results in black stripes between images
                        n = n + 1
                    except Exception as e:
                        print (str(e))
                print ("year " + str(year) + ", n = " + str(n))
                print ("images to mosaick: ")
                print (filter(os.path.isfile,glob.glob(jpgdir+"\*.jpg")))
                arcpy.MosaicToNewRaster_management(filter(os.path.isfile,glob.glob(jpgdir+"\*.jpg")), os.path.join(viewerdir,"150"),year+".jpg",srGoogle,"","","3","MINIMUM","MATCH")
                arcpy.MosaicToNewRaster_management(filter(os.path.isfile,glob.glob(jpgdir+"\*.jpg")), os.path.join(viewerdir,"150"),year+"-84.jpg",srWGS84,"","","3","MINIMUM","MATCH")
                metaitem['type'] = 'topo150'
                metaitem['imagename'] = year+'.jpg'
                desc = arcpy.Describe(os.path.join(viewerdir, "150", year+'-84.jpg'))
                metaitem['lat_sw'] = desc.extent.YMin
                metaitem['long_sw'] = desc.extent.XMin
                metaitem['lat_ne'] = desc.extent.YMax
                metaitem['long_ne'] = desc.extent.XMax

                os.remove(os.path.join(viewerdir,"150",year+"-84.jpg"))
                metadata.append(metaitem)
            arcpy.env.outputCoordinateSystem = None
            # arcpy.env.extent = None

            # write corner coordinates to Oracle

            if os.path.exists(os.path.join(viewer_path, OrderNumText+"_topo")):
                shutil.rmtree(os.path.join(viewer_path, OrderNumText+"_topo"))
            shutil.copytree(os.path.join(scratch, OrderNumText+"_topo"), os.path.join(viewer_path, OrderNumText+"_topo"))
            url = upload_link+"TopoUpload?ordernumber=" + OrderNumText
            urllib.urlopen(url)

        else:
            arcpy.AddMessage("No viewer is needed. Do nothing")

        try:
            con = cx_Oracle.connect(connectionString)
            cur = con.cursor()

            cur.execute("delete from overlay_image_info where  order_id = %s and (type = 'topo75' or type = 'topo150')" % str(OrderIDText))

            if needViewer == 'Y':
                for item in metadata:
                    cur.execute("insert into overlay_image_info values (%s, %s, %s, %.5f, %.5f, %.5f, %.5f, %s, '', '')" % (str(OrderIDText), str(OrderNumText), "'" + item['type']+"'", item['lat_sw'], item['long_sw'], item['lat_ne'], item['long_ne'],"'"+item['imagename']+"'" ) )
                con.commit()

        finally:
            cur.close()
            con.close()

        shutil.copy(os.path.join(scratch,docreport), os.path.join(reportcheckFolder, "TopographicMaps"))  # occasionally get permission denied issue here when running locally

        arcpy.SetParameterAsText(2, os.path.join(scratch,docreport))

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
        cur.callproc('eris_topo.InsertTopoAudit', (OrderIDText, 'python-Error Handling',pymsg))

    finally:
        cur.close()
        con.close()

    raise       # raise the error again

print ("Final TOPO report directory: " + (os.path.join(reportcheckFolder, "TopographicMaps", OrderNumText + "_US_Topo.docx")))
print ("__________DONE")