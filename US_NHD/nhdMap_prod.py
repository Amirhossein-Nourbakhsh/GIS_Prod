
#-------------------------------------------------------------------------------
# Name:        ERIS DATABASE MAP
# Purpose:    Generates the maps for the ERIS NHD Reprt.
#
# Author:      cchen
#
# Created:     18/01/2019
# Copyright:   (c) cchen 2019
# Licence:     <your licence>
#-------------------------------------------------------------------------------
import os,traceback,sys,timeit
import arcpy,shutil
import cx_Oracle,json
start1 = timeit.default_timer()
# ### ENVIRONMENTAL SETTING ####
arcpy.env.overwriteOutput = True
eris_nhdreport_path =r"GISData\NHDReport"
eris_report_path = r"GISData\ERISReport\ERISReport\PDFToolboxes"
class Machine:
    machine_test = r"\\cabcvan1gis006"
    machine_prod = r"\\cabcvan1gis007"
class Credential:
    oracle_test = r"ERIS_GIS/gis295@GMTESTC.glaciermedia.inc"
    oracle_production = r"ERIS_GIS/gis295@GMPRODC.glaciermedia.inc"
class ReportPath:
    instant_report_test = r"\\cabcvan1obi002\ErisData\Reports\test\instant_reports"
    instant_report_prod = r"\\cabcvan1obi002\ErisData\Reports\prod\instant_reports"
class Map(object):
    def __init__(self,mxdPath,dfname=''):
        self.mxd = arcpy.mapping.MapDocument(mxdPath)
        self.df= arcpy.mapping.ListDataFrames(self.mxd,('%s*')%(dfname))[0]
    def addLayer(self,lyr,workspace_path, dataset_name='',workspace_type=r"SHAPEFILE_WORKSPACE",add_position="TOP"):
        lyr = arcpy.mapping.Layer(lyr)
        if dataset_name !='':
            lyr.replaceDataSource(workspace_path, workspace_type, os.path.splitext(dataset_name)[0])
        arcpy.mapping.AddLayer(self.df, lyr, add_position)
    def zoomToTopLayer(self,position =0):
        self.df.extent = arcpy.mapping.ListLayers(self.mxd)[0].getExtent()
    def toJPEG(self,outputpath):
        arcpy.mapping.ExportToJPEG(
        self.mxd, outputpath, self.data_frame,self.map_export_width,self.map_export_height,
        self.resolution, self.world_file,self.color_mode, self.jpeg_compression_quality,self.progressive)
        return outputpath
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
        self.buffer = os.path.join(machine_path,eris_report_path,r'layer',r'buffer.lyr')
        self.nhdPoints = os.path.join(machine_path,eris_nhdreport_path,r"layer",r"NHDpoints.lyr")
        self.nhdHouse = os.path.join(machine_path,eris_nhdreport_path,r"layer",r"ProjectProperty.lyr")
        self.npllayer = os.path.join(machine_path,eris_nhdreport_path,r"layer",r"NPL Boundaries_true.lyr")
class MXD():
    def __init__(self,machine_path,code):
        self.machine_path = machine_path
        self.get(code)
    def get(self,code):
        machine_path = self.machine_path
        if code == 9093:
            self.mxdnhd = os.path.join(machine_path,eris_nhdreport_path,r'mxd',r'nhdReport.mxd')
class Oracle:
    # static variable: oracle_functions
    oracle_functions = {
    'getOrderInfo':"eris_gis.getOrderInfo",
    'geterispointdetails':"eris_gis.getErisPointDetails ",}
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
class TestConfig:
    machine_path=Machine.machine_test
    instant_reports =ReportPath.instant_report_test
    def __init__(self,code):
        machine_path=self.machine_path
        self.LAYER=LAYER(machine_path)
        self.MXD=MXD(machine_path,code)
class ProdConfig:
    machine_path=Machine.machine_prod
    instant_reports =ReportPath.instant_report_prod
    def __init__(self,code):
        machine_path=self.machine_path
        self.LAYER=LAYER(machine_path)
        self.MXD=MXD(machine_path,code)
def createBuffers(orderBuffers,output_folder,buffer_name=r"buffer_%s.shp"):
    buffer_dict={}
    buffer_sizes_dict ={}
    for i in range(len(orderBuffers)):
        buffer_dict[i]=createGeometry(eval(orderBuffers[i].values()[0])[0],r"polygon",output_folder,buffer_name%i)
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
def addNHDpoint(pointInfo,mxd,output_folder):
    out_points=r'points'
    out_pointsSHP = os.path.join(output_folder,out_points+".shp")
    erisPointsLayer = config.LAYER.nhdPoints
    erisIDs_4points = dict((int(_.get('DATASOURCE_POINTS')[0].get('ERIS_DATA_ID')),[('%s'%(_.get("MAP_KEY_LOC"))) if _.get("MAP_KEY_NO_TOT")==1 else ('%s(%s)'%(_.get("MAP_KEY_LOC"), _.get("MAP_KEY_NO_TOT"))),int(_.get("NHD_TYPE"))]) for _ in pointInfo)
    nhdpoints = dict((int(_.get('DATASOURCE_POINTS')[0].get('ERIS_DATA_ID')),(_.get("X"),_.get("Y"))) for _ in pointInfo)
    if erisIDs_4points != {}:
        arcpy.CreateFeatureclass_management(output_folder, out_points, "MULTIPOINT", "", "DISABLED", "DISABLED", arcpy.SpatialReference(4269))
        check_field = arcpy.ListFields(out_pointsSHP,"ERISID")
        if check_field==[]:
            arcpy.AddField_management(out_pointsSHP, "ERISID", "LONG", field_length='40')
        cursor = arcpy.da.InsertCursor(out_pointsSHP, ['SHAPE@','ERISID'])
        for point in nhdpoints.keys():
            cursor.insertRow([arcpy.Multipoint(arcpy.Array([arcpy.Point(*nhdpoints[point])]),arcpy.SpatialReference(4269)),point])
        del cursor
        check_field = arcpy.ListFields(out_pointsSHP,"nhd")
        if check_field==[]:
            arcpy.AddField_management(out_pointsSHP, "nhd", "LONG", field_length='4')
        check_field = arcpy.ListFields(out_pointsSHP,"mapkey")
        if check_field==[]:
            arcpy.AddField_management(out_pointsSHP, "mapkey", "TEXT", "", "", "20", "", "NULLABLE", "NON_REQUIRED", "")
        rows = arcpy.UpdateCursor(out_pointsSHP)
        for row in rows:
            row.mapkey = erisIDs_4points[row.ERISID][0]
            row.nhd = erisIDs_4points[row.ERISID][1]
            rows.updateRow(row)
        del rows
        mxd.addLayer(erisPointsLayer,output_folder,out_points)
    return erisPointsLayer
def getMaps(mxd, output_folder,map_name,buffer_dict, buffer_sizes_list,buffer_name=r"buffer_%s.shp"):
    temp = []
    if buffer_name.endswith(".shp"):
        buffer_name = buffer_name[:-4]
    bufferLayer = config.LAYER.buffer
    for i in buffer_dict.keys():
        if buffer_sizes_list[i]>=0.04:
            mxd.addLayer(bufferLayer,output_folder,buffer_name%(i))
        if i in buffer_dict.keys()[-1:]:
            mxd.zoomToTopLayer()
            mxd.df.scale = ((int(1.1*mxd.df.scale)/100)+1)*100
            arcpy.mapping.ExportToJPEG(mxd.mxd,os.path.join(output_folder,map_name),resolution =400)
            return os.path.join(output_folder,map_name)
def exportMap(mxd,output_folder,map_name,UTMzone,buffer_dict,buffer_sizes_list, buffer_name=r"buffer_%s.shp"):
    mxd.df.spatialReference = arcpy.SpatialReference('WGS 1984 UTM Zone %sN'%UTMzone)
    temp = getMaps(mxd, output_folder,map_name, buffer_dict, buffer_sizes_list)
    mxd.mxd.saveACopy(os.path.join(output_folder,"mxd.mxd"))
    return temp

if __name__ == '__main__':
    try:
        ##  INPUT ##################
        orderID = arcpy.GetParameterAsText(0).strip()
        scratch = arcpy.env.scratchFolder

        ## Server Setting####################
        config = TestConfig(9093)

        ####################################
        orderGeometry = r'orderGeometry.shp'
        orderGeometrySHP = os.path.join(scratch,orderGeometry)
        bufferMax = ''
        map_name = 'map_%s.jpg'
        buffer_name = "buffer_%s.shp"

        ## STEPS ##########################
        # 1  get order info by Oracle call
        orderInfo= Oracle('prod').call_function('getorderinfo',orderID)
        map_name = map_name%orderInfo[u'ORDER_NUM']

        # 2 create order geometry
        orderGeometrySHP= createGeometry(eval(orderInfo[u'ORDER_GEOMETRY'][u'CENTROID'])[0],r'POINT',scratch,orderGeometry)
        end = timeit.default_timer()
        arcpy.AddMessage( ('create geometry shp', round(end -start1,4)))
        start=end

        # 3 create buffers
        [buffers, buffer_sizes] = createBuffers(orderInfo['BUFFER_GEOMETRY'],scratch,buffer_name)
        end = timeit.default_timer()
        arcpy.AddMessage(('create buffer shps', round(end -start,4)))
        start=end

        # 4 Maps
        # 4-0 initial Map
        map1 =Map(config.MXD.mxdnhd)
        end = timeit.default_timer()
        arcpy.AddMessage(('4-0 initiate object Map', round(end -start,4)))
        start=end

        # 4-2 add ERIS points
        erisPointsInfo= Oracle('prod').call_function('geterispointdetails',orderID)
        erisPointsLayer=addNHDpoint(erisPointsInfo,map1,scratch)
        end = timeit.default_timer()
        arcpy.AddMessage(('4-3 add ERIS points to Map object', round(end -start,4)))
        start=end

        # 4-2 add Order Geometry
        geometryLayer = config.LAYER.nhdHouse
        map1.addLayer(geometryLayer,scratch,orderGeometry)
        map1.zoomToTopLayer()
        end = timeit.default_timer()
        arcpy.AddMessage(('4-2 add Geometry layer to Map object', round(end -start,4)))
        start=end
##
        # 4-3 Add Address n Order Number Turn on Layers
        map1.addTextoMap('Address',"Property Address: %s, %s, %s, %s"%(orderInfo['ADDRESS'],orderInfo['CITY'],orderInfo['PROVSTATE'],orderInfo['POSTALZIP']))
        end = timeit.default_timer()
        arcpy.AddMessage(('4-3 Add Address n turn on source layers', round(end -start,4)))
        start=end

        # 4-4 add Buffer Export Map
        zoneUTM = orderInfo['ORDER_GEOMETRY']['UTM_ZONE']
        map_output = exportMap(map1,scratch,map_name,zoneUTM,buffers,buffer_sizes,buffer_name)
        end = timeit.default_timer()
        arcpy.AddMessage(('4-4 maps to 3 pdfs', round(end -start,4)))
        start=end
        del map1,erisPointsLayer
        config = ProdConfig(9093)
        shutil.copy(map_output,config.instant_reports)
        arcpy.SetParameterAsText(1,map_output)
        end = timeit.default_timer()
        arcpy.AddMessage(('4 maps to reportcheck', round(end -start1,4)))

    except:
        tb = sys.exc_info()[2]
        tbinfo = traceback.format_tb(tb)[0]
        pymsg = "PYTHON ERRORS:\nTraceback info:\n %s \nError Info:\n %s"%(tbinfo,str(sys.exc_info()[1]))
        msgs = "ArcPy ERRORS:\n %s\n"%arcpy.GetMessages(2)
        arcpy.AddError("hit CC's error code in except: Order ID:%s"%orderID)
        arcpy.AddError(pymsg)
        arcpy.AddError(msgs)
        raise





















