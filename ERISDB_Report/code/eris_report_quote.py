
#-------------------------------------------------------------------------------
# Name:        ERIS DATABASE MAP
# Purpose:    Generates the maps for the ERIS DATABASE Reprt.
#
# Author:      cchen
#
# Created:     18/01/2019
# Copyright:   (c) cchen 2019
# Licence:     <your licence>
#-------------------------------------------------------------------------------
import os
import traceback
import timeit
import arcpy
import json
import cx_Oracle
import shutil
import urllib
import topo_image_path
start1 = timeit.default_timer()
arcpy.env.overwriteOutput = True

eris_report_path = r"gptools\ERISDB_Report"
us_topo_path =r"gptools\Topo_USA"
eris_aerial_ca_path = r"gptools\Aerial_CAN"
tifdir_topo = r"\\cabcvan1fpr009\USGS_Topo\USGS_currentTopo_Geotiff"
world_aerial_arcGIS_online_URL = r"https://services.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/0/query?f=json&returnGeometry=false&spatialRel=esriSpatialRelIntersects&maxAllowableOffset=0&geometryType=esriGeometryPoint&inSR=4326&outFields=SRC_DATE"

class Machine:
    machine_test = r"\\cabcvan1gis006"
    machine_prod = r"\\cabcvan1gis007"
class Credential:
    oracle_test = r"eris_gis/gis295@GMTESTC.glaciermedia.inc"
    oracle_production = r"eris_gis/gis295@GMPRODC.glaciermedia.inc"
class ReportPath:
	quote_report_test = r"\\cabcvan1eap006\ErisData\Reports\test\reportcheck\Quote"
	quote_report_prod = r"\\cabcvan1eap006\ErisData\Reports\prod\reportcheck\Quote"
	old_quote_report_prod = r"\\cabcvan1eap006\ErisData\Reports\prod\reportcheck\Eris"
	old_quote_report_test = r"\\cabcvan1eap006\ErisData\Reports\test\reportcheck\Eris"
class TestConfig:
    machine_path=Machine.machine_test
    quote_reports =ReportPath.quote_report_test
    old_quote_reports =ReportPath.old_quote_report_test
    def __init__(self,code):
        machine_path=self.machine_path
        self.LAYER=LAYER(machine_path)
        self.DATA=DATA(machine_path)
        self.MXD=MXD(machine_path,code)
class ProdConfig:
    machine_path=Machine.machine_prod
    quote_reports =ReportPath.quote_report_prod
    old_quote_reports =ReportPath.old_quote_report_prod
    def __init__(self,code):
        machine_path=self.machine_path
        self.LAYER=LAYER(machine_path)
        self.DATA=DATA(machine_path)
        self.MXD=MXD(machine_path,code)

class Map(object):
    def __init__(self,mxdPath,dfname=''):
        self.mxd = arcpy.mapping.MapDocument(mxdPath)
        self.df= arcpy.mapping.ListDataFrames(self.mxd,('%s*')%(dfname))[0]
    def addLayer(self,lyr,workspace_path, dataset_name='',workspace_type="SHAPEFILE_WORKSPACE",add_position="TOP"):
        lyr = arcpy.mapping.Layer(lyr)
        if dataset_name !='':
            lyr.replaceDataSource(workspace_path, workspace_type, os.path.splitext(dataset_name)[0])
        arcpy.mapping.AddLayer(self.df, lyr, add_position)
    def toScale(self,value):
        self.df.scale=value
        self.scale =self.df.scale
    def zoomToTopLayer(self,position =0):
        self.df.extent = arcpy.mapping.ListLayers(self.mxd)[0].getExtent()
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
        self.topowhite = os.path.join(machine_path,eris_report_path,'layer',"topo_white.lyr")
        self.road = os.path.join(machine_path,eris_report_path,r"layer","Roadadd_notransparency.lyr")
class DATA():
    def __init__(self,machine_path):
        self.machine_path = machine_path
        self.get()
    def get(self):
        machine_path = self.machine_path
        self.data_topo = os.path.join(machine_path,us_topo_path,"masterfile","CellGrid_7_5_Minute.shp")
        self.road = os.path.join(machine_path,eris_report_path,r"layer","US","Roads2.lyr")
class MXD():
    def __init__(self,machine_path,code):
        self.machine_path = machine_path
        self.get(code)
    def get(self,code):
        machine_path = self.machine_path
        if code == 9093:
            self.mxdtopo = os.path.join(machine_path,eris_report_path,r"mxd","USTopoMapLayoutCC.mxd")
            self.mxdbing = os.path.join(machine_path,eris_report_path,r"mxd","USBingMapLayoutCC.mxd")
            self.mxdMM = os.path.join(machine_path,eris_report_path,'mxd','USLayoutMMCC_SM.mxd')
        elif code == 9036:
            self.mxdtopo = os.path.join(machine_path,eris_report_path,r"mxd","TopoMapLayoutCC.mxd")
            self.mxdbing = os.path.join(machine_path,eris_report_path,r"mxd","BingMapLayoutCC.mxd")
            self.mxdaerial = os.path.join(machine_path,eris_aerial_ca_path,'mxd','Aerial_CA.mxd')
            self.mxdMM = os.path.join(machine_path,eris_report_path,'mxd','CADLayoutMMCC_SM.mxd')
        elif code == 9049:
            self.mxdtopo = os.path.join(machine_path,eris_report_path,r"mxd","TopoMapLayoutCC.mxd")
            self.mxdbing = os.path.join(machine_path,eris_report_path,r"mxd","USBingMapLayoutCC.mxd")
            self.mxdMM = os.path.join(machine_path,eris_report_path,'mxd','MXLayoutMMCC.mxd')
class Oracle:
    # static variable: oracle_functions
    oracle_functions = {
    'getorderinfo':"eris_gis.getOrderInfo",
    'printtopo':"eris_gis.printTopo",
    'geterispointdetails':"eris_gis.getErisPointDetails "}
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
                    output=json.loads(cursor.callfunc(func[0],outType,((str(orderID)),)).read())
                except ValueError:
                    output = cursor.callfunc(func[0],outType,((str(orderID)),)).read()
                except AttributeError:
                    output = cursor.callfunc(func[0],outType,((str(orderID)),))
            return output
        except cx_Oracle.Error as e:
            raise Exception(("Oracle Failure",e.message))
        except Exception as e:
            raise Exception(("JSON Failure",e.message))
        except NameError as e:
            raise Exception("Bad Function")
        finally:
            self.close_connection()

def createBuffers(orderBuffers,output_folder,buffer_name=r"buffer_%s.shp"):
    buffer_dict={}
    buffer_sizes_dict ={}
    for i in range(len(orderBuffers)):
        buffer_dict[i]=createGeometry(eval(orderBuffers[i].values()[0])[0],"polygon",output_folder,buffer_name%i)
        buffer_sizes_dict[i] =float(orderBuffers[i].keys()[0])
    return [buffer_dict,buffer_sizes_dict]
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
def addRoadLayer(mxd,buffer_name, output_folder):
    road_clip = r"road_clip"
    arcpy.Clip_analysis(config.DATA.road, buffer_name, os.path.join(output_folder,road_clip), "0.3 Meters")
    mxd.addLayer(config.LAYER.road,output_folder,road_clip)
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
def exportTopo(mxd,output_folder,geometry_name,geometry_type, output_pdf,unit_code,bufferSHP,UTMzone):
    geometryLayer = eval('config.LAYER.%s'%geometry_type.lower())
    addOrderGeometry(mxd,geometry_type,output_folder,geometry_name)
    mxd.df.spatialReference = arcpy.SpatialReference('WGS 1984 UTM Zone %sN'%UTMzone)
    if unit_code == 9093:
        topoLayer = config.LAYER.topowhite
        topolist = getCurrentTopo(config.DATA.data_topo,bufferSHP,output_folder)
        mxd.addTextoMap("Year", "Year: %s"%getTopoQuadnYear(topolist)[1])
        mxd.addTextoMap("Quadrangle","Quadrangle(s): %s"%getTopoQuadnYear(topolist)[0])
        for topo in topolist:
            mxd.addLayer(topoLayer,output_folder,topo.split('.')[0],"RASTER_WORKSPACE","BOTTOM")
    elif unit_code ==9049:
        mxd.addTextoMap("Logo", "\xa9 ERIS Information Inc.")
    mxd.toScale(24000) if mxd.df.scale<24000 else mxd.toScale(1.1*mxd.df.scale)
    mxd.resolution=300
    arcpy.mapping.ExportToPDF(mxd.mxd,output_pdf)
    mxd.mxd.saveACopy(os.path.join(output_folder,"maptopo.mxd"))
def getCurrentTopo(masterfile_topo,inputSHP,output_folder): # copy current topo images that intersect with input shapefile to output folder
    masterLayer_topo = arcpy.mapping.Layer(masterfile_topo)
    arcpy.SelectLayerByLocation_management(masterLayer_topo,'intersect',inputSHP)
    if(int((arcpy.GetCount_management(masterLayer_topo).getOutput(0))) ==0):
        return None
    else:
        cellids_selected = []
        rows = arcpy.SearchCursor(masterLayer_topo) # loop through the relevant records, locate the selected cell IDs
        for row in rows:
            cellid = str(int(row.getValue("CELL_ID")))
            cellids_selected.append(cellid)
        del row
        del rows
        masterLayer_topo = None
        infomatrix = []

        for cellid in cellids_selected:
            exec("info = topo_image_path.topo_%s"%(cellid))
            infomatrix.append(info)
        _=[]
        for item in infomatrix:
            tifname = item[0][0:-4]   # note without .tif part
            topofile = os.path.join(tifdir_topo,tifname+"_t.tif")
            year = item[1]

            if os.path.exists(topofile):
                if '.' in tifname:
                    tifname = tifname.replace('.','')
                temp = tifname.split('_')
                temp.insert(-2,item[1])
                newtopo = '_'.join(temp)+'.tif'
                shutil.copyfile(topofile,os.path.join(output_folder,newtopo))
                _.append(newtopo)
        return _
def getTopoYear(name):
    for year in list(reversed(range(1900,2020))):
        if str(year) in name:
            return str(year)
    return None
def getTopoQuadnYear(topo_filelist):
    quadrangles=set()
    year=set()
    for topo in topo_filelist:
        name = topo.split("_")
        for i in range(len(name)):
             year_value = getTopoYear(name[i])
             if year_value:
                quadrangles.add('%s,%s'%(' '.join([name[j] for j in range(1,i)]), name[0]))
                year.add(year_value)
                break
    return ('; '.join(quadrangles),'; '.join(year))
def exportAerial(mxd,output_folder,geometry_name,geometry_type,centroid,scale,output_pdf,UTMzone):
    geometryLayer = eval('config.LAYER.%s'%geometry_type.lower())
    addOrderGeometry(mxd,geometry_type,output_folder,geometry_name)
    mxd.addTextoMap("Year","Year: %s"%getWorldAerialYear(centroid))
    mxd.df.spatialReference = arcpy.SpatialReference('WGS 1984 UTM Zone %sN'%UTMzone)
    mxd.toScale(10000) if mxd.df.scale<10000 else mxd.toScale(1.1*mxd.df.scale)
    mxd.resolution=300
    arcpy.mapping.ExportToPDF(mxd.mxd,output_pdf)
    mxd.mxd.saveACopy(os.path.join(output_folder,"mapbing.mxd"))
def getWorldAerialYear((centroid_X,centroid_Y)):
    fsURL = r"https://services.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/0/query?f=json&returnGeometry=false&spatialRel=esriSpatialRelIntersects&maxAllowableOffset=0&geometryType=esriGeometryPoint&inSR=4326&outFields=SRC_DATE"
    params = urllib.urlencode({'geometry':{'x':float(centroid_X),'y':float(centroid_Y)}})
    resultBing = urllib.urlopen(fsURL,params).read()
    if "error" not in resultBing:
        for year in range(1900,2020):
            if str(year) in resultBing :
                return str(year)
    else:
        tries = 5
        key = False
        while tries >= 0:
            if "error" not in resultBing:
                for year in range(1900,2020):
                    if str(year) in resultBing:
                        return str(year)
            elif tries == 0:
                    return ""
            else:
                time.sleep(5)
                tries -= 1
if __name__ == '__main__':
    try:
        ##  INPUT ##################715636 715637 715638
        orderID = arcpy.GetParameterAsText(0).strip()
        code = arcpy.GetParameterAsText(1).strip()#
        scratch = arcpy.env.scratchFolder#

        ## PARAMETERS####################
        orderGeometry = r'orderGeometry.shp'
        orderGeometrySHP = os.path.join(scratch,orderGeometry)
        map_name = '%s_quote.pdf'
        mapMM_name = 'mapMM'
        bufferMax = ''
        buffer_name = "buffer_%s.shp"
        mapMM = os.path.join(scratch,mapMM_name)
        aerial_pdf = os.path.join(scratch,'mapbing.pdf')
        topo_pdf = os.path.join(scratch,"maptopo.pdf")
        pdfreport = os.path.join(scratch,map_name)
        ####################################

        ## Server Setting####################
        code = 9093 if code.strip().lower()=='usa' else 9036 if code.strip().lower()=='can' else 9049 if code.strip().lower()=='mex' else ValueError
        config = ProdConfig(code)

        ## STEPS ##########################
         # 1  get order info by Oracle call
        orderInfo= Oracle('prod').call_function('getorderinfo',orderID)
        end = timeit.default_timer()
        arcpy.AddMessage(('call oracle', round(end -start1,4)))
        start=end

        # 2 create order geometry
        orderGeometrySHP = createGeometry(eval(orderInfo[u'ORDER_GEOMETRY'][u'GEOMETRY'])[0],orderInfo[u'ORDER_GEOMETRY'][u'GEOMETRY_TYPE'],scratch,orderGeometry)
        end = timeit.default_timer()
        arcpy.AddMessage( (' create geometry shp', round(end -start,4)))
        start=end

        # 3 create buffers
        [buffers, buffer_sizes] = createBuffers(orderInfo['BUFFER_GEOMETRY'],scratch,buffer_name)
        end = timeit.default_timer()
        arcpy.AddMessage(('create buffer shps', round(end -start,4)))
        start=end

        # 4 Maps
        # 4-0 initial Map
        map1 =Map(config.MXD.mxdMM)
        end = timeit.default_timer()
        arcpy.AddMessage(('4-0 initiate object Map', round(end -start,4)))
        start=end

        if code == 9093:
            # 3.1 MAx Buffer
            bufferMax = os.path.join(scratch,buffer_name%(len(orderInfo['BUFFER_GEOMETRY'])+1))
            maxBuffer = max([float(_.keys()[0]) for _ in orderInfo['BUFFER_GEOMETRY']])
            maxBuffer ="%s MILE"%(2*maxBuffer if maxBuffer>0.2 else 2)
            arcpy.Buffer_analysis(orderGeometrySHP,bufferMax,maxBuffer)
            end = timeit.default_timer()
            arcpy.AddMessage(('create buffer shps', round(end -start,4)))
            start=end
            # 4-1 add Road US
            # addRoadLayer(map1, bufferMax,scratch)
            # end = timeit.default_timer()
            # arcpy.AddMessage(('4-1 clip and add road', round(end -start,4)))
            # start=end

        # 4-2 add Order Geometry
        addOrderGeometry(map1,orderInfo['ORDER_GEOMETRY']['GEOMETRY_TYPE'],scratch,orderGeometry)
        end = timeit.default_timer()
        arcpy.AddMessage( ('4-2 add Geometry layer to Map object', round(end -start,4)))
        start=end
##
        # 4-3 Add Address n Order Number Turn on Layers
        map1.addTextoMap('Address',"Address: %s, %s, %s"%(orderInfo['ADDRESS'],orderInfo["CITY"],orderInfo['PROVSTATE']))
        map1.addTextoMap("OrderNum","Order Number: %s"%orderInfo['ORDER_NUM'])
        map1.turnOnLayer()
        end = timeit.default_timer()
        arcpy.AddMessage(('4-3 Add Address n turn on source layers', round(end -start,4)))
        start=end


        # 4-4 add Buffer Export Map
        bufferLayer = config.LAYER.buffer
        for i in buffers.keys():
            if buffer_sizes[i]>=0.04:
                map1.addLayer(bufferLayer,scratch,"buffer_%s"%(i))
        map1.zoomToTopLayer(bufferLayer)
        map1.df.scale = ((int(1.1*map1.df.scale)/100)+1)*100
        unit = 'Kilometer' if code ==9036 else 'Mile'
        if buffer_sizes !={}:
            map1.addTextoMap("Map","Map : %s %s Radius"%(buffer_sizes[i],unit))
        else:
            i =0
        zoneUTM = orderInfo['ORDER_GEOMETRY']['UTM_ZONE']
        if zoneUTM<10:
            zoneUTM =' %s'%zoneUTM
        map1.df.spatialReference = arcpy.SpatialReference('WGS 1984 UTM Zone %sN'%zoneUTM)
        arcpy.mapping.ExportToPDF(map1.mxd,os.path.join(scratch,map_name%(i)),resolution =250)
        maplist = [os.path.join(scratch,map_name%(i))]

        end = timeit.default_timer()
        arcpy.AddMessage(('4-4 maps to 3 pdfs', round(end -start,4)))
        start=end

        scale = map1.df.scale
        del map1

        # 5 Aerial
        mapbing = Map(config.MXD.mxdbing)
        end = timeit.default_timer()
        arcpy.AddMessage(('5-1 inital aerial', round(end -start,4)))
        start=end
        mapbing.df.scale = scale
        mapbing.addTextoMap('Address',"Address: %s, %s, %s"%(orderInfo['ADDRESS'],orderInfo["CITY"],orderInfo['PROVSTATE']))
        mapbing.addTextoMap("OrderNum","Order Number: %s"%orderInfo['ORDER_NUM'])
        exportAerial(mapbing,scratch,orderGeometry,orderInfo['ORDER_GEOMETRY']['GEOMETRY_TYPE'],eval(orderInfo['ORDER_GEOMETRY']['CENTROID'].strip('[]')),scale, aerial_pdf,zoneUTM)
        del mapbing
        end = timeit.default_timer()
        arcpy.AddMessage(('5-2 aerial', round(end -start,4)))
        start=end

        # 6 Topo
        maptopo = Map(config.MXD.mxdtopo)
        end = timeit.default_timer()
        arcpy.AddMessage(('6-1 topo', round(end -start,4)))
        start=end
        maptopo.df.scale = scale
        maptopo.addTextoMap('Address',"Address: %s, %s"%(orderInfo['ADDRESS'],orderInfo['PROVSTATE']))
        maptopo.addTextoMap("OrderNum","Order Number: %s"%orderInfo['ORDER_NUM'])
        exportTopo(maptopo,scratch,orderGeometry,orderInfo['ORDER_GEOMETRY']['GEOMETRY_TYPE'],topo_pdf,code,bufferMax,zoneUTM)
        del maptopo,orderGeometry
        end = timeit.default_timer()
        arcpy.AddMessage(('6 Topo', round(end -start,4)))
        start=end

        # 7 Report
        maplist.append(aerial_pdf)
        maplist.append(topo_pdf)
        end = timeit.default_timer()
        arcpy.AddMessage(('7 maplist', round(end -start,4)))
        start=end

        pdfreport =pdfreport%(orderInfo['ORDER_NUM'])
        outputPDF = arcpy.mapping.PDFDocumentCreate(pdfreport)
        for page in maplist:
            outputPDF.appendPages(page)
        outputPDF.saveAndClose()
        config = ProdConfig(code)
        shutil.copy(pdfreport,config.quote_reports)
        shutil.copy(pdfreport,config.old_quote_reports)
        end = timeit.default_timer()
        arcpy.AddMessage(('7 Bundle', round(end -start,4)))
        start=end
        end = timeit.default_timer()
        arcpy.AddMessage(('7 Total Time', round(end -start1,4)))
        arcpy.SetParameterAsText(2,pdfreport)

    except:
        tb = sys.exc_info()[2]
        tbinfo = traceback.format_tb(tb)[0]
        pymsg = "PYTHON ERRORS:\nTraceback info:\n %s \nError Info:\n %s"%(tbinfo,str(sys.exc_info()[1]))
        msgs = "ArcPy ERRORS:\n %s\n"%arcpy.GetMessages(2)
        arcpy.AddError("hit CC's error code in except: OrderID %s"%orderID)
        arcpy.AddError(pymsg)
        arcpy.AddError(msgs)
        raise





















