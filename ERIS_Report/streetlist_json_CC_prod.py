#-------------------------------------------------------------------------------
# Name:        module1
# Purpose:
#
# Author:      cchen
#
# Created:     21/02/2019
# Copyright:   (c) cchen 2019
# Licence:     <your licence>
#-------------------------------------------------------------------------------
import os,traceback,sys,timeit
import arcpy,shutil
import cx_Oracle,json
start1 = timeit.default_timer()
# ### ENVIRONMENTAL SETTING ####
arcpy.Delete_management(r"in_memory")
arcpy.env.overwriteOutput = True
eris_nhdreport_path =r"gptools\NHDReport"
eris_report_path = r"gptools\ERISReport"
class Machine:
    machine_test = r"\\cabcvan1gis006"
    machine_prod = r"\\cabcvan1gis007"
class Credential:
    oracle_test = r"ERIS_GIS/gis295@GMTESTC.glaciermedia.inc"
    oracle_production = r"ERIS_GIS/gis295@GMPRODC.glaciermedia.inc"
class LAYER:
    streetlyr_US =  r"F:\gptools\ERISStreetlist\ERISSearch.gdb\tl_finalmerge"
    streetlyr_CA = r"F:\gptools\ERISStreetlist\ERISSearch.gdb\ERISRoads"
    streetlyr_MX = r"F:\gptools\ERISStreetlist\ERISSearch.gdb\MEX_street_all"

class Oracle:
    # static variable: oracle_functions
    oracle_functions = {
    'getorderinfo':"eris_gis.getOrderInfo"}
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
    def __init__(self,code):
        machine_path=self.machine_path
        self.LAYER=LAYER(machine_path)
class ProdConfig:
    machine_path=Machine.machine_prod
    def __init__(self,code):
        machine_path=self.machine_path
        self.LAYER=LAYER(machine_path)
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
def createMultiBufferRing(orderInfo_buffer,buffersizes,output_folder):
    buffer_coords ={}
    buffers ={}
    for _ in orderInfo_buffer:
        buffer_coords[float(_.keys()[0])] = eval(_[_.keys()[0]])[0]

    if len(buffersizes)!=1:
        for i in range(len(buffersizes)-1):
            radius1 = buffersizes[i]
            radius2= buffersizes[i+1]
            features = []
            array = arcpy.Array()
            sub_array = arcpy.Array()
            outputSHP = os.path.join(output_folder,"buffer_ring_%s.shp"%(i+1))

            for coords in buffer_coords[radius1]:
                sub_array.add(arcpy.Point(*coords))
            sub_array.add(sub_array.getObject(0))
            array.add(sub_array)
            if i ==0:
                polygon = arcpy.Polygon(array,arcpy.SpatialReference(4269))
                features.append(polygon)
                arcpy.CopyFeatures_management(features,os.path.join(output_folder,"buffer_ring_%s.shp"%(i)))
                buffers[radius1] =os.path.join(output_folder,"buffer_ring_%s.shp"%(i))
            for coords in buffer_coords[radius2]:
                sub_array.add(arcpy.Point(*coords))
            sub_array.add(sub_array.getObject(0))
            array.add(sub_array)
            polygon = arcpy.Polygon(array,arcpy.SpatialReference(4269))
            features.append(polygon)
            arcpy.CopyFeatures_management(features,outputSHP)
            buffers[radius2] =outputSHP
    else:
        i=0
        radius1 = buffersizes[i]
        features = []
        array = arcpy.Array()
        sub_array = arcpy.Array()
        outputSHP = os.path.join(output_folder,"buffer_ring_%s.shp"%(i+1))
        for coords in buffer_coords[radius1]:
            sub_array.add(arcpy.Point(*coords))
        sub_array.add(sub_array.getObject(0))
        array.add(sub_array)
        if i ==0:
            polygon = arcpy.Polygon(array,arcpy.SpatialReference(4269))
            features.append(polygon)
            arcpy.CopyFeatures_management(features,os.path.join(output_folder,"buffer_ring_%s.shp"%(i)))
            buffers[radius1] =os.path.join(output_folder,"buffer_ring_%s.shp"%(i))
    return buffers
def toJson(data):
    return  json.dumps(data)
def getStreetList(streetlyr, clip_buffer,unit1):
    arcpy.Delete_management(r'in_memory\temp')
    if unit1 == 9036:
        streetFieldName = "STREET"
    elif unit1 == 9093:
        streetFieldName = "FULLNAME"
    elif unit1 == 9035:
        streetFieldName = "NOMVIAL"
    try:
        streetLayer = arcpy.mapping.Layer(streetlyr)
        clippedStreet= r'in_memory\temp'
        arcpy.Clip_analysis(streetLayer, clip_buffer, clippedStreet)
    except:
        raise
    streetList = ""
    nSelected = int(arcpy.GetCount_management(clippedStreet).getOutput(0))
    if nSelected == 0 :
        streetList = ""
    else:
        streetArray = []
        rows = arcpy.SearchCursor(clippedStreet)
        for row in rows:
            value = row.getValue(streetFieldName)
            if(value.strip() != ''):
                streetArray.append(value.upper())
        streetSet = set(streetArray)
        streetList = r"|".join(streetSet)
        del row
        del rows

    del clippedStreet
    return streetList

if __name__ == '__main__':
    try:
        ##  INPUT ##################
        orderID = arcpy.GetParameterAsText(0).strip()
        code =arcpy.GetParameterAsText(1).strip()#'usa' #
        scratch =arcpy.env.scratchFolder#r"E:\CC\luan\test1"#
        scratchGDB =os.path.join(scratch,"scratch.gdb")
        arcpy.CreateFileGDB_management(scratch,"scratch.gdb")

        ## PARAMETERS####################
        buffer_ring = os.path.join(scratch,'buffer_ring.shp')
        bufferGeometrySHP = os.path.join(scratch,'buffer_max.shp')
        streetLyr_clipped = os.path.join(scratchGDB,'streetLyr_clipped')
        orderGeometry = r'orderGeometry.shp'
        orderGeometrySHP = os.path.join(scratch,orderGeometry)
        code = 9093 if code.strip().lower()=='usa' else 9036 if code.strip().lower()=='can' else 9035 if code.strip().lower()=='mex' else ValueError
        streetList={ "ORDER_ID":orderID,0: ""}
        ## Link To Server #################

        ## STEPS ##########################
         # 1  get order info by Oracle call
        orderInfo= Oracle('prod').call_function('getorderinfo',orderID)
        end = timeit.default_timer()
        arcpy.AddMessage(('call oracle', round(end -start1,4)))
        start=end

        # 2 create order geometry
        orderGeometrySHP= createGeometry(eval(orderInfo[u'ORDER_GEOMETRY']['GEOMETRY'])[0],orderInfo[u'ORDER_GEOMETRY']['GEOMETRY_TYPE'],scratch,orderGeometry)
        end = timeit.default_timer()
        arcpy.AddMessage( ('create geometry shp', round(end -start,4)))
        start=end

        # 3 create buffer rings
        buffer_sizes = [float(list(_)[0]) for _ in orderInfo['BUFFER_GEOMETRY']]
        buffers = createMultiBufferRing(orderInfo['BUFFER_GEOMETRY'],buffer_sizes,scratch)
        end = timeit.default_timer()
        arcpy.AddMessage( ('create Multiple Ring Buffer', round(end -start,4)))
        start=end

        streetLyr = LAYER.streetlyr_US if code == 9093 else LAYER.streetlyr_CA if code == 9036 else LAYER.streetlyr_MX if code == 9035 else ValueError
        arcpy.Buffer_analysis(orderGeometrySHP,bufferGeometrySHP,"%s Miles"%buffer_sizes[-1])
        arcpy.Clip_analysis(streetLyr, bufferGeometrySHP, streetLyr_clipped)
        end = timeit.default_timer()
        arcpy.AddMessage( ('clip street layer', round(end -start,4)))
        start=end

        for key in buffer_sizes:
            ring = arcpy.mapping.Layer(buffers[key])
            streetList[key] = getStreetList(streetLyr_clipped,ring,code)
            end = timeit.default_timer()
            arcpy.AddMessage( ('get Street List', round(end -start,4)))
            start=end
        if buffer_sizes==[] and orderInfo['ORDER_GEOMETRY']['GEOMETRY_TYPE']=='POLYGON':
            streetList[0] = getStreetList(streetLyr_clipped,orderGeometrySHP,0,code)
        if streetList[0] =='':
            streetList.pop(0)
        streetList_json = toJson(streetList)
        end = timeit.default_timer()
        arcpy.AddMessage(('return unplottables', round(end -start,4)))
        arcpy.SetParameterAsText(2,streetList_json)
    except:
        tb = sys.exc_info()[2]
        tbinfo = traceback.format_tb(tb)[0]
        pymsg = "PYTHON ERRORS:\nTraceback info:\n %s \nError Info:\n %s"%(tbinfo,str(sys.exc_info()[1]))
        msgs = "ArcPy ERRORS:\n %s\n"%arcpy.GetMessages(2)
        arcpy.AddError("order ID:%s hit CC's error code in except: "%orderID)
        arcpy.AddError(pymsg)
        arcpy.AddError(msgs)
        raise
