#-------------------------------------------------------------------------------
# Name:        module1
# Purpose:
#
# Author:      cchen
#
# Created:     05/03/2019
# Copyright:   (c) cchen 2019
# Licence:     <your licence>
#-------------------------------------------------------------------------------
import cx_Oracle
import os, arcpy
import traceback,sys,timeit
import contextlib
import ConfigParser
import zipfile

from PyPDF2 import PdfFileReader,PdfFileWriter
from PyPDF2.generic import NameObject, createStringObject, ArrayObject, FloatObject
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Frame,Table
from reportlab.lib.styles import getSampleStyleSheet,ParagraphStyle
from reportlab.lib.units import inch
from reportlab.lib.pagesizes import portrait, letter
from reportlab.pdfgen import canvas

import shutil
import json
import urllib

start1 = timeit.default_timer()
arcpy.env.overwriteOutput = True

def server_loc_config(configpath,environment):
    configParser = ConfigParser.RawConfigParser()
    configParser.read(configpath)
    if environment == 'test':
        reportcheck_test = configParser.get('server-config','reportcheck_test')
        reportviewer_test = configParser.get('server-config','reportviewer_test')
        reportinstant_test = configParser.get('server-config','instant_test')
        reportnoninstant_test = configParser.get('server-config','noninstant_test')
        upload_viewer = configParser.get('url-config','uploadviewer')
        server_config = {'reportcheck':reportcheck_test,'viewer':reportviewer_test,'instant':reportinstant_test,'noninstant':reportnoninstant_test,'viewer_upload':upload_viewer}
        return server_config
    elif environment == 'prod':
        reportcheck_prod = configParser.get('server-config','reportcheck_prod')
        reportviewer_prod = configParser.get('server-config','reportviewer_prod')
        reportinstant_prod = configParser.get('server-config','instant_prod')
        reportnoninstant_prod = configParser.get('server-config','noninstant_prod')
        upload_viewer = configParser.get('url-config','uploadviewer_prod')
        server_config = {'reportcheck':reportcheck_prod,'viewer':reportviewer_prod,'instant':reportinstant_prod,'noninstant':reportnoninstant_prod,'viewer_upload':upload_viewer}
        return server_config
    else:
        return 'invalid server configuration'

eris_report_path = r"gptools\ERISReport"
eris_aerial_ca_path = r"gptools\Aerial_CAN"
server_environment = 'prod'
server_config_file = r'\\cabcvan1gis007\gptools\ERISServerConfig.ini'
server_config = server_loc_config(server_config_file,server_environment)

class Machine:
    machine_test = r"\\cabcvan1gis006"
    machine_prod = r"\\cabcvan1gis007"

class Credential:
    oracle_test = r"ERIS_GIS/gis295@GMTESTC.glaciermedia.inc"
    oracle_production = r'eris_gis/gis295@cabcvan1ora003.glaciermedia.inc:1521/GMPRODC' #"ERIS_GIS/gis295@GMPRODC.glaciermedia.inc"

class ReportPath:
    caaerial_prod= r"\\CABCVAN1OBI007\ErisData\prod\aerial_ca"
    caaerial_test= r"\\CABCVAN1OBI007\ErisData\test\aerial_ca"
    pdfreport_test= os.path.join(server_config['reportcheck'],"AerialsDigital")
    pdfreport_prod = os.path.join(server_config['reportcheck'],"AerialsDigital")

class CoverpagePath:
    cover_aerial_test = r"\\cabcvan1gis006\GISData\Aerial_CAN\mxd\ERIS_2018_ReportCover_Historical Aerials_F.jpg"
    summary_aerial_test = r"\\cabcvan1gis006\GISData\Aerial_CAN\mxd\ERIS_2018_ReportCover_Second Page_F.jpg"
    cover_aerial_prod = r"\\cabcvan1gis007\gptools\Aerial_CAN\mxd\ERIS_2018_ReportCover_Historical Aerials_F.jpg"
    summary_aerial_prod = r"\\cabcvan1gis007\gptools\Aerial_CAN\mxd\ERIS_2018_ReportCover_Second Page_F.jpg"

class TestConfig:
    machine_path=Machine.machine_test
    caaerial_path = ReportPath.caaerial_test
    cover_aerial_path = CoverpagePath.cover_aerial_test
    summary_aerial_path = CoverpagePath.summary_aerial_test
    pdfreport_aerial_path = ReportPath.pdfreport_test
    def __init__(self):
        machine_path=self.machine_path
        self.LAYER=LAYER(machine_path)
        self.MXD=MXD(machine_path)
#        self.LOGO = LOGOFILE(machine_path,code)

class ProdConfig:
    machine_path=Machine.machine_prod
    caaerial_path = ReportPath.caaerial_prod
    cover_aerial_path = CoverpagePath.cover_aerial_prod
    summary_aerial_path = CoverpagePath.summary_aerial_prod
    pdfreport_aerial_path = ReportPath.pdfreport_prod
    def __init__(self):
        machine_path=self.machine_path
        self.LAYER=LAYER(machine_path)
        self.MXD=MXD(machine_path)
#        self.LOGO = LOGOFILE(machine_path,code)

class Map(object):
    def __init__(self,mxdPath,dfname=''):
        self.mxd = arcpy.mapping.MapDocument(mxdPath)
        self.df= arcpy.mapping.ListDataFrames(self.mxd,('%s*')%(dfname))[0]
    def addLayer(self,lyr,workspace_path='', dataset_name='',workspace_type="FILEGDB_WORKSPACE",add_position="TOP"):
        lyr = arcpy.mapping.Layer(lyr)
        if dataset_name !='':
            lyr.replaceDataSource(workspace_path, workspace_type, os.path.splitext(dataset_name)[0])
        arcpy.mapping.AddLayer(self.df, lyr, add_position)
    def replaceLayerSource(self,lyr_name,to_path, dataset_name='',workspace_type="FILEGDB_WORKSPACE"):
        for _ in arcpy.mapping.ListLayers(self.mxd):
            if _.name == lyr_name:
                _.replaceDataSource(to_path, workspace_type,dataset_name)
                return
        arcpy.mapping.ListLayers(self.mxd)[0].replaceDataSource(to_path, workspace_type,dataset_name)
    def toScale(self,value):
        self.df.scale=value
        self.scale =self.df.scale
    def zoomToTopLayer(self,position =0):
        self.df.extent = arcpy.mapping.ListLayers(self.mxd)[position].getExtent()
    def turnOnLayer(self):
        layers = arcpy.mapping.ListLayers(self.mxd, "*", self.df)
        for layer in layers:
            layer.visible = True
        arcpy.RefreshTOC()
        arcpy.RefreshActiveView()
    def addTextoMap(self,textName,textValue, x=None,y=None):
        textElements =arcpy.mapping.ListLayoutElements(self.mxd,"TEXT_ELEMENT")
        for element in textElements:
            if textName.lower() in (element.name).lower():
                element.text = textValue
                if x!=None or y!=None:
                    element.elementPositionX=x
                    element.elementPositionY=y
    def zoomToExtent(self,default_proj = 3395,default_scale = 10000):
        self.df.spatialReference = arcpy.SpatialReference(default_proj)
        self.zoomToTopLayer()
        scale = map1.df.scale
        self.toScale(default_scale) if self.df.scale <=default_scale else self.toScale(self.df.scale*1.1)

class LAYER():
    def __init__(self,machine_path):
        self.machine_path = machine_path
        self.get()
    def get(self):
        machine_path = self.machine_path
        self.buffer = os.path.join(machine_path,eris_report_path,'layer','buffer.lyr')
        self.point = os.path.join(machine_path,eris_report_path,r"layer","SiteMaker.lyr")
        self.polyline = os.path.join(machine_path,eris_report_path,r"layer","orderLine.lyr")
        self.polygon = os.path.join(machine_path,eris_report_path,r"layer","orderPoly.lyr")
        self.buffer = os.path.join(machine_path,eris_report_path,'layer','buffer.lyr')
        self.grid = os.path.join(machine_path,eris_report_path,r"layer","GridCC.lyr")
        self.aeriallyr_oneband_bk = os.path.join(machine_path,eris_aerial_ca_path,'mxd',"one_band_Nodata_black.lyr")
        self.aeriallyr_threeband_bk = os.path.join(machine_path,eris_aerial_ca_path,'mxd',"three_band_Nodata_black.lyr")
        self.aeriallyr_oneband_tr = os.path.join(machine_path,eris_aerial_ca_path,'mxd',"one_band_Nodata_transparent.lyr")
        self.aeriallyr_threeband_tr = os.path.join(machine_path,eris_aerial_ca_path,'mxd',"three_band_Nodata_transparent.lyr")
        self.aeriallyr_oneband_tr_xp =os.path.join(machine_path,eris_aerial_ca_path,'mxd',"one_band_xp.lyr")
        self.aeriallyr_threeband_tr_xp =os.path.join(machine_path,eris_aerial_ca_path,'mxd',"three_band_xp.lyr")

class MXD():
    def __init__(self,machine_path):
        self.machine_path = machine_path
        self.get()
    def get(self):
        machine_path = self.machine_path
        self.mxdaerial = os.path.join(machine_path,eris_aerial_ca_path,'mxd','Aerial_CA.mxd')
        self.mxdaerial_mm = os.path.join(machine_path,eris_aerial_ca_path,'mxd','Aerial_CA_MM.mxd')

class Oracle:
    # static variable: oracle_functions
    oracle_functions = {'getorderinfo':"eris_gis.getOrderInfo",
    'getcoverpageinfo':"eris_gis.getCoverPageInfo",
    "getorderdecades":"eris_aerial_can.getOrderDecades",
    "getselectedlist":"eris_aerial_can.getSelectedList",
    "getlist":"eris_aerial_can.getList",
    'setaeriallistcan':'eris_aerial_can.setAerialListCan',
    "isinhouse":"eris_aerial_can.isInHouse",
    "isgeoreferenced":"eris_aerial_can.isGeoreferenced"
    }
    def __init__(self,machine_name):
        # initiate connection credential
        if machine_name.lower() =='test':
            self.oracle_credential = Credential.oracle_test
        elif machine_name.lower()=='prod':
            self.oracle_credential = Credential.oracle_production
        else:
            raise ValueError("Bad machine name")
    def connect_to_oracle(self):
        try:
            self.oracle_connection = cx_Oracle.connect(self.oracle_credential)
            self.cursor = self.oracle_connection.cursor()
            self.commit = self.oracle_connection.commit()
        except cx_Oracle.Error as e:
            print(e,'Oralce connection failed, review credentials.')
    def close_connection(self):
        self.cursor.close()
        self.oracle_connection.close()
    def call_function(self,function_name,orderID):
        self.connect_to_oracle()
        cursor = self.cursor
        try:
            outType = cx_Oracle.CLOB
            func = [self.oracle_functions[_] for _ in self.oracle_functions.keys() if function_name.lower() ==_.lower()]
            if func !=[] and len(func)==1:
                try:
                    if type(orderID) !=list:
                        orderID = [orderID]
                    output=json.loads(cursor.callfunc(func[0],outType,orderID).read())
                except ValueError:
                    output = cursor.callfunc(func[0],outType,orderID).read()
                except AttributeError:
                    output = cursor.callfunc(func[0],outType,orderID)
            return output
        except cx_Oracle.Error as e:
            raise Exception(("Oracle Failure",e.message.message))
        except Exception as e:
            raise Exception(("JSON Failure",e.message.message))
        except NameError as e:
            raise Exception("Bad Function")
        finally:
            self.close_connection()
    def pass_values(self,function_name,value):#(self,function_name,data_type,value):
        self.connect_to_oracle()
        cursor = self.cursor
        try:
            func = [self.oracle_functions[_] for _ in self.oracle_functions.keys() if function_name.lower() ==_.lower()]
            if func !=[] and len(func)==1:
                try:
                    #output= cursor.callfunc(func[0],oralce_object,value)
                    output= cursor.callproc(func[0],value)
                    return 'pass'
                except ValueError:
                    raise
            return 'failed'
        except cx_Oracle.Error as e:
            raise Exception(("Oracle Failure",e.message.message))
        except Exception as e:
            raise Exception(e.message)
        except NameError as e:
            raise Exception("Bad Function")
        finally:
            self.close_connection()

def createGeometry(pntCoords,geometry_type,output_folder,output_name, spatialRef = arcpy.SpatialReference(4269)):
    outputSHP = os.path.join(output_folder,output_name)
    if geometry_type.lower()== 'point':
        arcpy.CreateFeatureclass_management(output_folder, output_name, "MULTIPOINT", "", "DISABLED", "DISABLED", spatialRef)
        cursor = arcpy.da.InsertCursor(outputSHP, ['SHAPE@'])
        cursor.insertRow([arcpy.Multipoint(arcpy.Array([arcpy.Point(*coords) for coords in pntCoords]),spatialRef)])
    elif geometry_type.lower() =='polyline':
        arcpy.CreateFeatureclass_management(output_folder, output_name, "POLYLINE", "", "DISABLED", "DISABLED", spatialRef)
        cursor = arcpy.da.InsertCursor(outputSHP, ['SHAPE@'])
        cursor.insertRow([arcpy.Polyline(arcpy.Array([arcpy.Point(*coords) for coords in pntCoords]),spatialRef)])
    elif geometry_type.lower() =='polygon':
        arcpy.CreateFeatureclass_management(output_folder,output_name, "POLYGON", "", "DISABLED", "DISABLED", spatialRef)
        cursor = arcpy.da.InsertCursor(outputSHP, ['SHAPE@'])
        cursor.insertRow([arcpy.Polygon(arcpy.Array([arcpy.Point(*coords) for coords in pntCoords]),spatialRef)])
    del cursor
    return outputSHP

def addOrderGeometry(mxd,geometry_type,output_folder,name):
    geometryLayer = eval('config.LAYER.%s'%(geometry_type.lower()))
    if arcpy.mapping.ListLayoutElements(mxd.mxd, "LEGEND_ELEMENT", "Legend") !=[]:
        legend = arcpy.mapping.ListLayoutElements(mxd.mxd, "LEGEND_ELEMENT", "Legend")[0]
        legend.autoAdd = True
        mxd.addLayer(geometryLayer,output_folder,name)
        legend.autoAdd = False
    else:
        mxd.addLayer(geometryLayer,output_folder,name)
    mxd.zoomToTopLayer()

def createAnnotPdf(geom_type, myShapePdf):
    # part 1: read geometry pdf to get the vertices and rectangle to use
    source  = PdfFileReader(open(myShapePdf,'rb'))
    geomPage = source.getPage(0)
    mystr = geomPage.getObject()['/Contents'].getData()
    #to pinpoint the string part: 1.19997 791.75999 m 1.19997 0.19466 l 611.98627 0.19466 l 611.98627 791.75999 l 1.19997 791.75999 l
    #the format seems to follow x1 y1 m x2 y2 l x3 y3 l x4 y4 l x5 y5 l
    geomString = mystr.split('S\r\n')[0].split('M\r\n')[1]
    coordsString = [value for value in geomString.split(' ') if value not in ['m','l','']]
    # part 2: update geometry in the map

    if geom_type.upper() == 'POLYGON':
        pdf_geom = PdfFileReader(open(annot_poly,'rb'))
    elif geom_type.upper() == 'POLYLINE':
        pdf_geom = PdfFileReader(open(annot_line,'rb'))
    page_geom = pdf_geom.getPage(0)
    annot = page_geom['/Annots'][0]
    updateVertices = "annot.getObject().update({NameObject('/Vertices'):ArrayObject([FloatObject("+coordsString[0]+")"

    for item in coordsString[1:]:
        updateVertices = updateVertices + ',FloatObject('+item+')'
    updateVertices = updateVertices + "])})"
    exec(updateVertices)
    xcoords = []
    ycoords = []

    for i in range(0,len(coordsString)-1):
        if i%2 == 0:
            xcoords.append(float(coordsString[i]))
        else:
            ycoords.append(float(coordsString[i]))
    # below rect seems to be geom bounding box coordinates: xmin, ymin, xmax,ymax
    annot.getObject().update({NameObject('/Rect'):ArrayObject([FloatObject(min(xcoords)), FloatObject(min(ycoords)), FloatObject(max(xcoords)), FloatObject(max(ycoords))])})
    annot.getObject().pop('/AP')  # this is to get rid of the ghost shape
    annot.getObject().update({NameObject('/T'):createStringObject(u'ERIS')})
    output = PdfFileWriter()
    output.addPage(page_geom)
    annotPdf = os.path.join(scratch, "annot.pdf")
    outputStream = open(annotPdf,"wb")
    # output.setPageMode('/UseOutlines')
    output.write(outputStream)
    outputStream.close()
    output = None
    return annotPdf

def annotatePdf(mapPdf, myAnnotPdf):
    pdf_intermediate = PdfFileReader(open(myAnnotPdf,'rb'))
    page= pdf_intermediate.getPage(0)
    pdf = PdfFileReader(open(mapPdf,'rb'))
    FIMpage = pdf.getPage(0)
    page.mergePage(FIMpage)
    output = PdfFileWriter()
    output.addPage(page)
    annotatedPdf = mapPdf[:-4]+'_a.pdf'
    outputStream = open(annotatedPdf,"wb")
    # output.setPageMode('/UseOutlines')
    output.write(outputStream)
    outputStream.close()
    output = None
    return annotatedPdf

def myCoverPage(canv, doc):
    global coverInfotext
    canv.drawImage(config.cover_aerial_path,0,0, PAGE_WIDTH,PAGE_HEIGHT)
    canv.saveState()
    leftsw= 54
    heights = 400
    rightsw = 200
    space = 20
    canv.setFont('Helvetica-Bold', 13)
    canv.drawString(leftsw, heights, "Project Property:")
    canv.drawString(leftsw, heights-3*space,"Project No:")
    canv.drawString(leftsw, heights-4*space,"Requested By:")
    canv.drawString(leftsw, heights-5*space,"Order No:")
    canv.drawString(leftsw, heights-6*space,"Date Completed:")
    canv.setFont('Helvetica', 13)
    canv.drawString(rightsw,heights-0*space, coverInfotext["SITE_NAME"])
    canv.drawString(rightsw, heights-1*space,coverInfotext["ADDRESS"].split("\n")[0])
    canv.drawString(rightsw, heights-2*space,coverInfotext["ADDRESS"].split("\n")[1])
    canv.drawString(rightsw, heights-3*space,coverInfotext["PROJECT_NUM"])
    canv.drawString(rightsw, heights-4*space,coverInfotext["COMPANY_NAME"])
    canv.drawString(rightsw, heights-5*space,coverInfotext["ORDER_NUM"])
    canv.drawString(rightsw, heights-6*space,time.strftime('%B %d, %Y', time.localtime()))

    if NRF=='Y':
        canv.setStrokeColorRGB(0.67,0.8,0.4)
        canv.line(50,180,PAGE_WIDTH-60,180)
        canv.setFont('Helvetica-Bold', 12)
        canv.drawString(70,160,"Please note that no information was found for your site or adjacent properties.")
    canv.restoreState()
    del canv

def goCoverPage(coverPdf):
    doc = SimpleDocTemplate(coverPdf, pagesize = letter)
    doc.build([Spacer(0,4*inch)],onFirstPage=myCoverPage, onLaterPages=myCoverPage)
    doc = None

def myFirstPage(canv, doc):
    global sub_metadata_list
    canv.drawImage(config.summary_aerial_path,0,0, int(PAGE_WIDTH),int(PAGE_HEIGHT))
    canv.saveState()
    leftsw= 95
    heights =  PAGE_HEIGHT - 130
    rightsw = 200
    inline_space = 95
    space = 20
    canv.setFont('Helvetica-Bold', 12)
    i=0

    for title in ['Decade','Year','Image Scale','Source']:
        canv.drawString(leftsw+i*inline_space, heights, title)
        i+=1
    heights = heights-space
    canv.setFont('Helvetica', 10)

    for i in range(len(sub_metadata_list)):
        for j in range(len(sub_metadata_list[i])):
            canv.drawString(leftsw+j*inline_space, heights-i*space, unicode(sub_metadata_list[i][j]))

    canv.setFont('Helvetica', 8)
    canv.drawString(54, 160, "Aerial Maps included in this report are produced by the sources listed above and are to be used for research purposes including a phase I report. ")
    canv.drawString(54, 150,"Maps are not to be resold as commercial property. No warranty of Accuracy or Liability for ERIS: The information contained in this report ")
    canv.drawString(54, 140,"has been produced by ERIS Information Inc.(in the US) and ERIS Information Limited Partnership (in Canada), both doing business and ERIS ")
    canv.drawString(54, 130,"Information Limited Partnership (in Canada), both doing business as 'ERIS', using aerial photos listed in above sources. The maps contained in this ")
    canv.drawString(54, 120,"report does not purport to be and does not constitute a guarantee of the accuracy of the information contained herein. Although ERIS has endeavored ")
    canv.drawString(54,110," to present you with information that is accurate, ERIS disclaims, any and all liability for any errors, omissions, or inaccuracies in such ")
    canv.drawString(54,100,"information and data, whether attributable to inadvertence, negligence or otherwise, and for any consequences arising therefrom. Liability ")
    canv.drawString(54,90,"on the part of ERIS is limited to the monetary value paid for this report.")
    canv.showPage()
    p=None
    Footer = None
    Disclaimer = None
    style = None
    del canv

def goSummaryPage(summaryPdf):
    doc = SimpleDocTemplate(summaryPdf, pagesize = letter)
    doc.build([Spacer(0,4*inch)],onFirstPage=myFirstPage, onLaterPages=myFirstPage)
    doc = None

def zipdir(path, zip):
    for root, dirs, files in os.walk(path):
        for file in files:
            # print file + " " + root
            arcname = os.path.relpath(os.path.join(root, file), os.path.join(path, '..'))
            zip.write(os.path.join(root, file), arcname)

# ENVIRONMENTAL SETTING
arcpy.Delete_management(r"in_memory")
arcpy.env.overwriteOutput = True

styles = getSampleStyleSheet()
pagesize = portrait(letter)
[PAGE_WIDTH,PAGE_HEIGHT]=pagesize[:2]

# -----------------------------------------------------------------------------------------------------------------------
if __name__ == '__main__':
    start = timeit.default_timer()

    # ENVIRONMENTAL SETTING
    orderID = ""#arcpy.GetParameterAsText(0)
    OrderNumText = r"21021800285"
    yesBoundary = 'no'#arcpy.GetParameterAsText(1)#'yes'#'no'#
    multipage = False# True if arcpy.GetParameterAsText(2).lower()=='yes' or arcpy.GetParameterAsText(2).lower()=='y' else False
    grids = 3 #arcpy.GetParameterAsText(3)
    scratch =  r"\\cabcvan1gis005\MISC_DataManagement\_AW\AERIAL_CA_SCRATCHY\%s" %OrderNumText #arcpy.env.scratchFolder#r"E:\CC\luan\test1"#
    scratch_mxd = os.path.join(scratch,"mxd_template.mxd")
    if not os.path.exists(scratch):
        os.mkdir(scratch)

    # GET ORDER_ID FROM ORDER_NUM
    if orderID == "":
        oracle = Oracle('prod')
        oracle.connect_to_oracle()
        cur = oracle.cursor
        cur.execute("select order_id from orders where order_num = '" + str(OrderNumText) + "'")
        result = cur.fetchall()
        orderID = str(result[0][0]).strip()
        print("Order ID: " + orderID)
        oracle.close_connection()

    # create Scratch GDB
    scratchGDB =os.path.join(scratch,r"scratch.gdb")
    if not os.path.exists(scratchGDB):
        arcpy.CreateFileGDB_management(scratch,r"scratch.gdb")

    # Parameter SETTING
    defaultscale = 10000 #defualt 10k
    imageMeta ={}
    metadata_list=[]
    orderGeometry = r'orderGeometry'
    Gridlr ="gridlr"
    coverPDF = os.path.join(scratch,"cover.pdf")
    summarypdf = os.path.join(scratch,'summary%s.pdf')
    pdfreport = os.path.join(scratch,"%s_CA_Aerial.pdf")

    # Job Follder SETTING
    orderInfo = Oracle('prod').call_function('getorderinfo',orderID)
    OrderNumText = str(orderInfo["ORDER_NUM"])
    config=ProdConfig()
    job_folder = os.path.join(config.caaerial_path,OrderNumText)
    image_folder = os.path.join(job_folder,'org')
    work_folder_georeferenced = r"\\CABCVAN1OBI007\ErisData\Work_aerial\georeferenced"
    work_folder_nongeoreferenced = r"\\CABCVAN1OBI007\ErisData\Work_aerial\non_georeferenced"
    nongeoreference_folder = r"\\cabcvan1fpr009\NAPL\non_georeferenced"
    footprint_work_folder1 = r"\\CABCVAN1OBI007\ErisData\Work_aerial\footprint\fin_footprint.gdb"
    viewer_path = server_config['viewer']
    upload_link = server_config['viewer_upload']+r"/ErisInt/BIPublisherPortal_prod/Viewer.svc/CAAerialUPload?ordernumber="
    annot_poly = r"\\cabcvan1gis007\gptools\Aerial_CAN\mxd\annot_poly.pdf"
    annot_line = r"\\cabcvan1gis007\gptools\Aerial_CAN\mxd\annot_line.pdf"

# MAIN ---------------------------------------------------------------------------------------------------------------------
# 1 create Order
    try:
        NRF = 'N'
        orderGeometrySHP =  createGeometry(eval(orderInfo[u'ORDER_GEOMETRY'][u'GEOMETRY'])[0],orderInfo[u'ORDER_GEOMETRY'][u'GEOMETRY_TYPE'],scratchGDB,orderGeometry)

        selectedlist = Oracle('prod').call_function('getselectedlist',orderID)
        if selectedlist==[]:
            NRF = 'Y'

        for item in selectedlist:
            [decade,imagename,availability,year,scale,source,comments] = [item['DECADE'],item['IMAGE_NAME'],item['IN_HOUSE'],item['YEAR'],item['SCALE'],item['SOURCE'],item['COMMENTS']]

            if os.path.exists(os.path.join(work_folder_georeferenced,imagename)):
                roll_photo = imagename[:-7]
                arcpy.Copy_management(os.path.join(work_folder_georeferenced,imagename),os.path.join(image_folder,imagename))

            if decade not in imageMeta.keys():
                imageMeta[decade] = [[availability,imagename,year,scale,source,comments]]
            else:
                templist = imageMeta[decade]
                templist.append([availability,imagename,year,scale,source,comments])
                imageMeta[decade] =templist

        if yesBoundary =='tiff':
            tiffolder = os.path.join(scratch,OrderNumText+"_CA_Aerial")
            if not os.path.exists(tiffolder):
                os.mkdir(tiffolder)
            maplist = {}

            for decade in sorted(imageMeta.keys()):
                for [_,imagename,year,scale,_,comment] in imageMeta[decade]:
                    imagename=imagename.replace("_gc.tif",".tif")
                    if os.path.exists(os.path.join(image_folder,imagename)):
                        shutil.copy(os.path.join(image_folder,imagename),os.path.join(tiffolder,'aerial_%s_%s_%s%s'%(year,scale,comment,imagename[-4:])))
                    elif os.path.exists(os.path.join(work_folder_nongeoreferenced,imagename)):
                        shutil.copy(os.path.join(work_folder_nongeoreferenced,imagename),os.path.join(tiffolder,'aerial_%s_%s_%s%s'%(year,scale,comment,imagename[-4:])))
                    elif os.path.exists(os.path.join(nongeoreference_folder,imagename)):
                        shutil.copy(os.path.join(nongeoreference_folder,imagename),os.path.join(tiffolder,'aerial_%s_%s_%s%s'%(year,scale,comment,imagename[-4:])))
                    else:
                        raise Exception("no image found %s"%imagename)
            
            myZipFile = zipfile.ZipFile(tiffolder+".zip","w")
            zipdir(tiffolder,myZipFile)
            myZipFile.close()
            shutil.copy(tiffolder+".zip",config.pdfreport_aerial_path)
            arcpy.SetParameterAsText(4,tiffolder+".zip")
        else:
            if multipage:
                arcpy.Copy_management(config.MXD.mxdaerial_mm,scratch_mxd)
            else:
                arcpy.Copy_management(config.MXD.mxdaerial,scratch_mxd)

            map1 =Map(scratch_mxd)
            map1.addTextoMap('Address',"%s, %s"%(orderInfo['ADDRESS'],orderInfo['PROVSTATE']))
            map1.addTextoMap("OrderNum","Order Number: %s"%orderInfo['ORDER_NUM'])

            addOrderGeometry(map1,orderInfo[u'ORDER_GEOMETRY'][u'GEOMETRY_TYPE'],scratchGDB,orderGeometry)

            if multipage:
                maplistMM={}
                ddmmaa = map1.mxd.dataDrivenPages
                arcpy.GridIndexFeatures_cartography(os.path.join(scratchGDB,Gridlr), orderGeometrySHP, "", "", "", number_rows =grids,number_columns = grids)
                # arcpy.GridIndexFeatures_cartography(os.path.join(scratchGDB,Gridlr), orderGeometrySHP, "", "", "", polygon_width="8000 Meters",polygon_height="8000 Meters")
                newgridlr = arcpy.mapping.ListLayers(map1.mxd,"Grid",map1.df)[0]
                newgridlr.visible =True

                # -------------------------------------------------------------------------------------------------------
                # SKIP BLANK GRIDS FOR MULTIPAGES
                for decade in sorted(imageMeta.keys()):
                    imageslist = [[_[0],os.path.join(image_folder,_[1]),_[2]] for _ in imageMeta[decade]]
                    decadefootprint = os.path.join(scratchGDB,"footprint_"+str(decade))
                    arcpy.CreateFeatureclass_management(scratchGDB, "footprint_"+str(decade), "POLYGON", "", "DISABLED", "DISABLED", arcpy.SpatialReference(4269))
                     
                    for [availability,imagepath,year] in imageslist:                
                        print(imagepath.split("\\")[-1].replace("_gc.tif", ""))
                        print(decade)
                        elevRaster = arcpy.sa.Raster(imagepath)
                        myExtent = elevRaster.extent

                        # Create a polygon geometry
                        array = arcpy.Array([arcpy.Point(myExtent.XMin, myExtent.YMin),
                                            arcpy.Point(myExtent.XMin, myExtent.YMax),
                                            arcpy.Point(myExtent.XMax, myExtent.YMax),
                                            arcpy.Point(myExtent.XMax, myExtent.YMin),
                                            arcpy.Point(myExtent.XMin, myExtent.YMin)
                            ])
                        polygon = arcpy.Polygon(array)
  
                        # Open an InsertCursor and insert the new geometry
                        cursor = arcpy.da.InsertCursor(decadefootprint, ['SHAPE@'])
                        cursor.insertRow([polygon])
                        del cursor
                
                    arcpy.MakeFeatureLayer_management(os.path.join(scratchGDB,Gridlr), 'gridlr')
                    arcpy.MakeFeatureLayer_management(decadefootprint, 'decadefootprint')
                    arcpy.SelectLayerByLocation_management('gridlr', 'intersect', 'decadefootprint')

                    gridyear = os.path.join(scratchGDB,'grid_'+str(decade))
                    arcpy.CopyFeatures_management('gridlr', gridyear)             
                # -------------------------------------------------------------------------------------------------------                        

            if yesBoundary !='fixed' :
                for lyr in arcpy.mapping.ListLayers(map1.mxd, "" ,map1.df):
                    lyr.visible=False
            arcpy.RefreshTOC()

            maplist = {}
            layerlist = []

            # SPLIT DECADE -------------------
            splityears = []
            if splityears:
                splitlist = [imageMeta.get(key) for key in splityears][0]
                for y in splityears:
                    del imageMeta[y]

                for item in splitlist:
                    if item[2] not in imageMeta.keys():
                        imageMeta[item[2]] = [item]
                    else:
                        imageMeta[item[2]].append(item)
            # --------------------------------

            for decade in sorted(imageMeta.keys()):
                map_pdf =os.path.join(scratch,'map_%s.pdf'%decade)
                count = 0
                imageslist = [[_[0],os.path.join(image_folder,_[1]),_[2]] for _ in imageMeta[decade]]
                yearlist = [str(_) for _ in set([_[2] for _ in imageMeta[decade]])]
                scalelist = set(["1: %s"%_[3] for _ in imageMeta[decade]])
                sourcelist = set([_[4] for _ in imageMeta[decade]])
                commentslist = set([_[5] for _ in imageMeta[decade] if _[5]!=""])

                for [_,_,year,scale,source,_] in imageMeta[decade]:
                    decades = int(math.floor(decade / 10.0)) * 10
                    metadata_list.append([decades,year,scale,source])

                for [availability,imagepath,year] in imageslist:
                    if availability =='Y' and os.path.exists(imagepath):
                        count +=1

                        if arcpy.Describe(imagepath).spatialReference.factoryCode ==0:
                            arcpy.DefineProjection_management(imagepath,arcpy.SpatialReference(4326))

                        image = arcpy.MakeRasterLayer_management(imagepath,"image")
                        tempresult = arcpy.GetRasterProperties_management(imagepath, "BANDCOUNT")

                        if int(tempresult.getOutput(0)) == 1:
                            layerfile = config.LAYER.aeriallyr_oneband_tr if len(imageslist)>1 else config.LAYER.aeriallyr_oneband_bk
                        else:
                            layerfile = config.LAYER.aeriallyr_threeband_tr if len(imageslist)>1 else config.LAYER.aeriallyr_threeband_bk
                        
                        arcpy.ApplySymbologyFromLayer_management("image", layerfile)
                        layer_temp = os.path.join(scratch,"%s_%s.lyr"%(year,count))
                        arcpy.SaveToLayerFile_management('image',layer_temp)
                        map1.addLayer(layer_temp,add_position="BOTTOM")
                        arcpy.Delete_management('image')
                        layerlist.append(layer_temp)
                    else:
                        arcpy.AddMessage("not available yet: %s"%imagepath)

                map1.zoomToExtent(default_scale = defaultscale)
                map1.addTextoMap("Year",'; '.join(yearlist))
                map1.addTextoMap("Scale","1: %s"%int(map1.df.scale))#("Scale",'; '.join(scalelist))
                map1.addTextoMap("Source",'; '.join(sourcelist))
                map1.addTextoMap("Comments",'; '.join(commentslist) if commentslist != set([]) else " ")
                                
                if multipage:
                    newgridlr.replaceDataSource(scratchGDB,"FILEGDB_WORKSPACE",'grid_'+str(decade))
                    arcpy.mapping.ExportToPDF(map1.mxd, map_pdf, "PAGE_LAYOUT", 640, 480, 600, "BEST", "RGB", True, "ADAPTIVE", "RASTERIZE_BITMAP", False, True, "NONE", True, 70)
                    ddmmaa.refresh()
                    ddmmaa.exportToPDF(map_pdf.replace(".pdf","_all.pdf"),"ALL",resolution=600)
                    maplistMM[decade] =map_pdf.replace(".pdf","_all.pdf")
                else:
                    arcpy.mapping.ExportToPDF(map1.mxd, map_pdf, "PAGE_LAYOUT", 640, 480, 600, "BEST", "RGB", True, "ADAPTIVE", "RASTERIZE_BITMAP", False, True, "NONE", True, 70)

                maplist[decade]=map_pdf
                map1.zoomToExtent(default_proj=4326,default_scale = defaultscale)
                map1.mxd.saveACopy(os.path.join(scratch, "mxd_%s.mxd"%decade))

                i=2 if multipage else 1
                for lyr in arcpy.mapping.ListLayers(map1.mxd, "" ,map1.df)[i:]:
                    arcpy.mapping.RemoveLayer(map1.df, lyr)
                arcpy.RefreshTOC()

            if yesBoundary.lower() == 'yes' and orderInfo[u'ORDER_GEOMETRY'][u'GEOMETRY_TYPE'].lower() != "point":
                shapePdf = os.path.join(scratch, 'shape.pdf')

                for lyr in arcpy.mapping.ListLayers(map1.mxd, "" ,map1.df)[:1]:
                # for lyr in arcpy.mapping.ListLayers(map1.mxd, "" ,map1.df):
                    lyr.visible = True
                if multipage:
                    newgridlr.visible = True
                    del newgridlr

                map1.zoomToExtent()
                arcpy.mapping.ExportToPDF(map1.mxd, shapePdf, "PAGE_LAYOUT", 640, 480, 600, "BEST", "RGB", True, "ADAPTIVE", "RASTERIZE_BITMAP", False, True, "NONE", True, 50)
                myAnnotPdf = createAnnotPdf(orderInfo[u'ORDER_GEOMETRY'][u'GEOMETRY_TYPE'], shapePdf)
                
                for decade in sorted(maplist.keys()):
                    maplist[decade] = annotatePdf(maplist[decade], myAnnotPdf)

            del map1
            print(metadata_list)
            print(maplist)

#-----------------------------------------------------------------------------------------------------------------------
            coverInfotext = Oracle('prod').call_function('getcoverpageinfo',orderID)
            coverInfotext["ADDRESS"] = '%s\n%s %s %s'%(coverInfotext["ADDRESS"],coverInfotext["CITY"],coverInfotext["PROVSTATE"],coverInfotext["POSTALZIP"])
            goCoverPage(coverPDF)
            
            if NRF=='Y':
                os.rename(coverPDF,pdfreport%OrderNumText)
            else:
                summary_pages = []
                selected_decade_ist = set([int(_[0]) for _ in metadata_list])
                
                for decade in [int(_) for _ in Oracle('prod').call_function('getorderdecades',orderID) if int(_) not in selected_decade_ist]:
                    metadata_list.append([decade,"Not Available"," "," "])

                metadata_list = sorted(metadata_list, key = lambda x: int(x[0]))

                for i in range(0, len(metadata_list), 24):
                    sub_metadata_list =  metadata_list[i:i + 24]
                    goSummaryPage(summarypdf%i)
                    summary_pages.append(summarypdf%i)
                    
                output = PdfFileWriter()
                coverPages = PdfFileReader(open(coverPDF,'rb'))
                output.addPage(coverPages.getPage(0))
                output.addBookmark("Cover Page",0)
                del coverPages

                for j in range(len(summary_pages)):
                    summaryPages = PdfFileReader(open(summary_pages[j],'rb'))
                    output.addPage(summaryPages.getPage(0))
                    output.addBookmark("Summary Page",1+j)
                del summaryPages

                i = 1+j
                for decade in sorted(maplist.keys()):
                    mapPage = PdfFileReader(open(maplist[decade],'rb'))

                    for k in range(mapPage.getNumPages()):
                        output.addPage(mapPage.getPage(k))
                        output.addBookmark("%ss"%decade,i+k+1)

                    if multipage:
                        i =i+k+1
                        mapPage = PdfFileReader(open(maplistMM[decade],'rb'))

                        for l in range(mapPage.getNumPages()):
                            output.addPage(mapPage.getPage(l))
                        i = i+l
                    i+=1
                    del mapPage

                output.setPageMode('/UseOutlines')
                outputStream = open(pdfreport%OrderNumText,"wb")
                output.write(outputStream)
                outputStream.close()
                del output,outputStream

            shutil.copy(pdfreport%OrderNumText,job_folder)
            needViewer = 'N'
            
            try:
                oracle =Oracle('prod')
                oracle.connect_to_oracle()
                cur = oracle.cursor
                cur.execute("select aerial_viewer from order_viewer where order_id =" + str(orderID))
                t = cur.fetchone()

                if t != None:
                    needViewer = t[0]
            finally:
                oracle.close_connection()

            arcpy.AddMessage("Need Xplorer %s"%needViewer)
            if needViewer == 'Y':
                viewer_dir = os.path.join(scratch, OrderNumText+'_caaerial')
                metadata = []

                if not os.path.exists(viewer_dir):
                    os.mkdir(viewer_dir)

                if not multipage:
                    for decade in sorted(imageMeta.keys()):
                        yearlist = list(set([str(imageMeta[decade][i][2]) for i in range(len(imageMeta[decade]))]))

                        if len(yearlist)==1:
                            imagename = r"%s.jpg"%yearlist[0]
                        elif len(yearlist) >1:
                            imagename = r"%s%s.jpg"%(yearlist[0][-2:],yearlist[1][-2:])

                        mxd = arcpy.mapping.MapDocument(os.path.join(scratch, "mxd_%s.mxd"%decade))
                        df = arcpy.mapping.ListDataFrames(mxd,"*")[0]
                        arcpy.mapping.ExportToJPEG(mxd, os.path.join(viewer_dir,imagename),df,5100,6000, world_file = True, jpeg_quality=50)
                        del mxd,df

                        extent = arcpy.Describe(os.path.join(viewer_dir,imagename)).extent#.projectAs(arcpy.SpatialReference(4326))
                        metaitem = {}
                        metaitem['type'] = 'caaerial'
                        metaitem['imagename'] = imagename
                        metaitem['lat_sw'] = extent.YMin
                        metaitem['long_sw'] = extent.XMin
                        metaitem['lat_ne'] = extent.YMax
                        metaitem['long_ne'] = extent.XMax
                        metadata.append(metaitem)
                        arcpy.AddMessage( 'CA Aerial Xplorer metadata: %s'%metaitem)
                else:
                    for layerpath in layerlist:
                        layer = arcpy.mapping.Layer(layerpath)
                        tempresult = arcpy.GetRasterProperties_management(layer.dataSource, "BANDCOUNT")

                        if int(tempresult.getOutput(0)) == 1:
                            layerfile = config.LAYER.aeriallyr_oneband_tr_xp
                        else:
                            layerfile = config.LAYER.aeriallyr_threeband_tr_xp

                        arcpy.ApplySymbologyFromLayer_management(layer, layerfile)
                        layer_temp = os.path.join(scratch,layerpath.split("\\")[-1].replace(".lyr","_1.lyr"))
                        arcpy.SaveToLayerFile_management(layer,layer_temp)
                        map1 =Map(scratch_mxd)
                        map1.addLayer(layer_temp,add_position="TOP")
                        map1.df.elementHeight =8/(float(arcpy.GetRasterProperties_management(layer.dataSource,"COLUMNCOUNT")[0])/float(arcpy.GetRasterProperties_management(layer.dataSource,"ROWCOUNT")[0]))
                        map1.zoomToExtent(4326)

                        imagename = layerpath.split("\\")[-1].replace(".lyr",".jpg")
                        arcpy.mapping.ExportToJPEG(map1.mxd, os.path.join(viewer_dir, imagename), map1.df,df_export_width= 6000,df_export_height=6000,world_file = True, jpeg_quality=70)
                        extent =arcpy.Describe(os.path.join(viewer_dir,imagename)).extent#.projectAs(arcpy.SpatialReference(4326))

                        metaitem = {}
                        metaitem['type'] = 'caaerial'
                        metaitem['imagename'] = imagename
                        metaitem['lat_sw'] = extent.YMin
                        metaitem['long_sw'] = extent.XMin
                        metaitem['lat_ne'] = extent.YMax
                        metaitem['long_ne'] = extent.XMax
                        metadata.append(metaitem)
                        arcpy.AddMessage( 'CA Aerial Xplorer metadata: %s'%metaitem)
                        del map1

                if os.path.exists(os.path.join(viewer_path, OrderNumText+"_caaerial")):
                    shutil.rmtree(os.path.join(viewer_path, OrderNumText+"_caaerial"))

                shutil.copytree(viewer_dir, os.path.join(viewer_path, OrderNumText+"_caaerial"))
                url = upload_link + OrderNumText
                contextlib.closing(urllib.urlopen(url))

            try:
                oracle =Oracle('prod')
                oracle.connect_to_oracle()
                cur = oracle.cursor
                cur.execute("delete from overlay_image_info where  order_id = %s and (type = 'caaerial')" % str(orderID))

                if needViewer == 'Y':
                    for item in metadata:
                        cur.execute("insert into overlay_image_info values (%s, %s, %s, %.5f, %.5f, %.5f, %.5f, %s, '', '')" % (str(orderID), str(OrderNumText), "'" + item['type']+"'", item['lat_sw'], item['long_sw'], item['lat_ne'], item['long_ne'],"'"+item['imagename']+"'" ) )
                    oracle.oracle_connection.commit()
            finally:
                oracle.close_connection()

            shutil.copyfile(pdfreport%OrderNumText,os.path.join(config.pdfreport_aerial_path,OrderNumText+"_CA_Aerial.pdf"))
            arcpy.SetParameterAsText(4,pdfreport%OrderNumText)

    except:
        tb = sys.exc_info()[2]
        tbinfo = traceback.format_tb(tb)[0]
        pymsg = "PYTHON ERRORS:\nTraceback info:\n %s \nError Info:\n %s"%(tbinfo,str(sys.exc_info()[1]))
        msgs = "ArcPy ERRORS:\n %s\n"%arcpy.GetMessages(2)
        arcpy.AddError("hit CC's error code in except: OrderID %s"%orderID)
        arcpy.AddError(pymsg)
        arcpy.AddError(msgs)
        raise

# print("DONE")
# print(os.path.join(config.pdfreport_aerial_path,OrderNumText+"_CA_Aerial.pdf"))