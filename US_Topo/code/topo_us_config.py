import os
import sys
import arcpy
import ConfigParser

file_path =os.path.dirname(os.path.abspath(__file__))
if 'arcgisserver' in file_path:
    model_path = os.path.join(r'\\cabcvan1gis007\arcgisserver\directories\arcgissystem\arcgisinput\GPtools_temp\DB_Framework')
else:
    main_path = os.path.abspath(os.path.join(__file__, os.pardir))
    model_path = os.path.join(main_path.split('GIS_Prod')[0],'GIS_Prod','DB_Framework')

sys.path.insert(1,model_path)
import models

def server_loc_config(configpath,environment):
    configParser = ConfigParser.RawConfigParser()
    configParser.read(configpath)
    dbconnection = configParser.get('server-config','dbconnection_%s'%environment)
    reportcheck = configParser.get('server-config','reportcheck_%s'%environment)
    reportviewer = configParser.get('server-config','reportviewer_%s'%environment)
    reportinstant = configParser.get('server-config','instant_%s'%environment)
    reportnoninstant = configParser.get('server-config','noninstant_%s'%environment)
    upload_viewer = configParser.get('url-config','uploadviewer_%s'%environment)
    server_config = {'dbconnection':dbconnection,'reportcheck':reportcheck,'viewer':reportviewer,'instant':reportinstant,'noninstant':reportnoninstant,'viewer_upload':upload_viewer}
    return server_config

# def createScratch():
#     scratch = r'C:\Users\JLoucks\Documents\JL\test1'
#     scratchgdb = "scratch.gdb"
#     if not os.path.exists(scratch):
#         os.mkdir(scratch)
#     if not os.path.exists(os.path.join(scratch, scratchgdb)):
#         arcpy.CreateFileGDB_management(scratch, "scratch.gdb")
#     return scratch, scratchgdb

##arcpy parameters
OrderIDText = arcpy.GetParameterAsText(0)
BufsizeText = arcpy.GetParameterAsText(1)
yesBoundary = 'yes' if arcpy.GetParameterAsText(2).lower()=='yes' or arcpy.GetParameterAsText(2).lower()=='arrow' else ('fixed' if arcpy.GetParameterAsText(2).lower()=='fixed' else 'no')
multipage = arcpy.GetParameterAsText(3)
gridsize = arcpy.GetParameterAsText(4)
scratch = arcpy.env.scratchFolder
scratchgdb = arcpy.env.scratchGDB

# OrderIDText = '1214789'
# BufsizeText = '2.4'
# yesBoundary = 'yes'#'yes' if arcpy.GetParameterAsText(2).lower()=='yes' or arcpy.GetParameterAsText(2).lower()=='arrow' else ('fixed' if arcpy.GetParameterAsText(2).lower()=='fixed' else 'no')
# gridsize = '0'
# multipage = 'no'
# scratch,scratchgdb = createScratch()

# order info
order_obj = models.Order().get_order(OrderIDText)

# # flags
# multipage = "Y"                     # Y/N, for multipages
# gridsize =  0 # "3 KiloMeters"           # for multipage grid
# yesBoundary = "yes"                 # fixed/yes/no
# BufsizeText = "2.4"
# delyearFlag = "N"                   # Y/N, for internal use only, blank maps, etc.

# scratch file/folder outputs
# scratch, scratchgdb = createScratch()
summaryPdf = os.path.join(scratch,'summary.pdf')
coverPdf = os.path.join(scratch,"cover.pdf")
shapePdf = os.path.join(scratch, 'shape.pdf')
annotPdf = os.path.join(scratch, "annot.pdf")
orderGeometry = os.path.join(scratch, scratchgdb, "orderGeometry")
orderGeometryPR = os.path.join(scratch, scratchgdb, "orderGeometryPR")
orderBuffer = os.path.join(scratch, scratchgdb, "buffer")
extent = os.path.join(scratch, scratchgdb, "extent")

# connections/report outputs
server_environment = 'prod'
serverpath = r"\\cabcvan1gis007"
server_config_file = os.path.join(serverpath, r"gptools\ERISServerConfig.ini")
server_config = server_loc_config(server_config_file,server_environment)

reportcheckFolder = server_config["reportcheck"]
noninstantFolder = server_config["noninstant"]
viewerFolder = server_config["viewer"]
topouploadurl =  server_config["viewer_upload"] + r"/ErisInt/BIPublisherPortal_prod/Viewer.svc/TopoUpload?ordernumber="
connectionString = server_config["dbconnection"]

# folders
connectionPath = os.path.join(serverpath, r"gptools\Topo_USA\python")
mxdpath = os.path.join(connectionPath, r"mxd")

# master data files\folders
masterfolder = r'\\cabcvan1fpr009\USGS_Topo'
# csvfile_h = os.path.join(masterfolder, r"USGS_MapIndice\All_HTMC_all_all_gda_results.csv")
# csvfile_c = os.path.join(masterfolder,r"USGS_MapIndice\All_USTopo_T_7.5_gda_results.csv")
# mastergdb = os.path.join(masterfolder, r"USGS_MapIndice\MapIndices_National_GDB.gdb")
csvfile_h = r"\\cabcvan1gis007\gptools\Topo_USA\masterfile\All_HTMC_all_all_gda_results.csv"
csvfile_c = r"\\cabcvan1gis007\gptools\Topo_USA\masterfile\All_USTopo_T_7.5_gda_results.csv"
mastergdb = r"\\cabcvan1gis007\gptools\Topo_USA\masterfile\Cell_PolygonAll.shp"
tifdir_h = os.path.join(masterfolder, "USGS_HTMC_Geotiff")
tifdir_c = os.path.join(masterfolder, "USGS_currentTopo_Geotiff")

# mxds
mxdfile = os.path.join(mxdpath,"template_new.mxd")
mxdfile_nova = os.path.join(mxdpath,'template_nova_new.mxd')
mxdfile_terracon = os.path.join(mxdpath,'template_terracon.mxd')
mxdfile_partner = os.path.join(mxdpath,'template_partner.mxd')

# layers
topolyrfile_none = os.path.join(mxdpath,"topo.lyr")
topolyrfile_b = os.path.join(mxdpath,"topo_black.lyr")
topolyrfile_w = os.path.join(mxdpath,"topo_white.lyr")
bufferlyrfile = os.path.join(mxdpath,"buffer.lyr")
orderGeomlyrfile_point = os.path.join(mxdpath,"orderPoint.lyr")
orderGeomlyrfile_polyline = os.path.join(mxdpath,"orderLine.lyr")
orderGeomlyrfile_polygon = os.path.join(mxdpath,"orderPoly.lyr")
orderGeomlyrred_polyline = os.path.join(mxdpath,"orderLine_red.lyr")
orderGeomlyrred_polygon = os.path.join(mxdpath,"orderPoly_red.lyr")
gridlyr = os.path.join(mxdpath,"grid.lyr")

# pdfs
annot_poly = os.path.join(mxdpath,"annot_poly.pdf")
annot_line = os.path.join(mxdpath,"annot_line.pdf")
annot_point = os.path.join(mxdpath,"annot_point.pdf")
annot_poly_c = os.path.join(mxdpath,"annot_poly_red.pdf")
annot_line_c = os.path.join(mxdpath,"annot_line_red.pdf")
annot_poly_y = os.path.join(mxdpath,"annot_poly_yellow.pdf")
annot_line_y = os.path.join(mxdpath,"annot_line_yellow.pdf")

# logos
logopath = os.path.join(mxdpath,"logos")

# covers
coverPic = os.path.join(mxdpath, "coverPic", "ERIS_2018_ReportCover_Topographic Maps_F.jpg")
summarypage = os.path.join(mxdpath, "coverPic", "ERIS_2018_ReportCover_Second Page_F.jpg")

# other
logfile = os.path.join(connectionPath, r"log\USTopoSearch_Log.txt")
logname = "TOPO_US_%s"%server_environment
readmefile = os.path.join(mxdpath,"readme.txt")