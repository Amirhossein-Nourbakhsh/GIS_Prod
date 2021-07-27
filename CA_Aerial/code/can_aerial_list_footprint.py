#-------------------------------------------------------------------------------
# Name:        module1
# Purpose:
#
# Author:      cchen
#
# Created:     15/09/2019
# Copyright:   (c) cchen 2019
# Licence:     <your licence>
#-------------------------------------------------------------------------------
import timeit,os,arcpy,cx_Oracle
import json
import contextlib
import urllib
arcpy.env.overwriteOutput = True
class Credential:
    oracle_test = r"ERIS_GIS/gis295@GMTESTC.glaciermedia.inc"
    oracle_production = r"ERIS_GIS/gis295@GMPRODC.glaciermedia.inc"
class Oracle:

    # static variable: oracle_functions
    oracle_functions = { 'getorderinfo':"eris_gis.getOrderInfo",
    "getselectedlist":"eris_aerial_can.getSelectedList",
    'setaeriallistcan':'eris_aerial_can.setAerialListCan'
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
            raise Exception(("JSON Failure",e.message))
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
class MasterNAPL:
    work_footprintGDB_orig = r"\\CABCVAN1OBI007\ErisData\Work_aerial\footprint\orig_footprint.gdb"
    napl_gdb = r"\\cabcvan1gis007\gptools\Aerial_CAN\masterfile\napl.gdb"
    napl_all = os.path.join(napl_gdb,"footprint_napl_all")
    napl_georeferenced = os.path.join(napl_gdb,"footprint_napl_georeferenced")
    job_prod_path = r"\\CABCVAN1OBI007\ErisData\prod\aerial_ca"

class Aerial:
    def __init__(self,ID,OrderNum):
        self.template_insertImage={
            "ORDER_ID" : int(ID),
            "ORDER_NUM":OrderNum,
            "IMAGE_NAME" : ""}
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

def fromInhouse(masterfile,whereclause,rollphoto,output):
    try:
        con = Oracle('prod')
        con.connect_to_oracle()
        cur = con.cursor
        cur.execute("select * from ERIS.AERIAL_INVENTORY where "+whereclause)
        item = list(cur.fetchone())
        item[10] = str(item[10].read())
        [attribute,shape] = [item[1:-2],eval(item[10])]
    finally:
        con.close_connection()

    if not arcpy.Exists(output):
        try:
            arcpy.CreateFeatureclass_management(MasterNAPL.work_footprintGDB_orig,rollphoto, "POLYGON", "", "DISABLED", "DISABLED", arcpy.SpatialReference(4326))
            field_names = field_names = ['Date','Roll','Photo','Color','Title','Year','In_house','Georeferenced','Scale','Envelope_box','Envelope_box_georeferenced','DPI']
            for field in field_names:
               arcpy.AddField_management(output,field,"TEXT",field_length =256 )
            cursor = arcpy.da.InsertCursor(output, ['SHAPE@']+field_names)
            cursor.insertRow(shape+attribute)
            del cursor
        except:
            arcpy.Delete_management(output)
def getSubmasterfile(jobgdb,output,masterfile,scratch):
    tempGDB =os.path.join(scratch,r"temp.gdb")
    if not os.path.exists(tempGDB):
        arcpy.CreateFileGDB_management(scratch,r"temp.gdb")
    footprint = os.path.join(tempGDB,output)

    if not arcpy.Exists(footprint):
        buff = os.path.join(jobgdb,r'bufferGeometry')
        if not arcpy.Exists(buff):
            orderGeometrySHP = os.path.join(tempGDB,r'orderGeometry')
            orderGeometrySHP =  createGeometry(eval(orderInfo[u'ORDER_GEOMETRY'][u'GEOMETRY'])[0],orderInfo[u'ORDER_GEOMETRY'][u'GEOMETRY_TYPE'],tempGDB,r'orderGeometry')
            arcpy.Buffer_analysis(orderGeometrySHP,buff,"0.5 KILOMETERS")
        meta = arcpy.mapping.Layer(masterfile)
        arcpy.SelectLayerByLocation_management(meta,"INTERSECT",buff)
        if int(arcpy.GetCount_management(meta)[0])!=0:
            arcpy.CopyFeatures_management(meta,footprint)
    return footprint
def copyFoottoWork(roll,photo,georef,bufferGeometrySHP):
    start = timeit.default_timer()
    # get footprint to job folder
    roll_photo = "_".join([roll,photo])
    footprint_path = os.path.join(MasterNAPL.work_footprintGDB_orig,roll_photo)
    if arcpy.Exists(footprint_path):
        arcpy.Delete_management(footprint_path)
    if not arcpy.Exists(footprint_path):
##        if georef:
##            master = getSubmasterfile(jobGDB,'nap_geo',MasterNAPL.napl_georeferenced,scratch)
##            where_clause = " roll_num= '%s' and photo_num = '%s'"%(roll,photo)
##            fromInhouse(master,where_clause,roll_photo,footprint_path)
##        else:
        master = getSubmasterfile(jobGDB,'nap_nongeo',MasterNAPL.napl_all,scratch)
        where_clause = where_clause = "Roll= '%s' and Photo = '%s'"%(roll,photo)
        fromInhouse(master,where_clause,roll_photo,footprint_path)
        end = timeit.default_timer()
        arcpy.AddMessage(('copy  footprint: %s'%roll_photo, round(end -start,4)))
    return roll_photo
if __name__ == '__main__':
    start1 = timeit.default_timer()
#    #### ENVIRONMENTAL SETTING ####
    orderID =r'826271'#arcpy.GetParameterAsText(0)#
    scratch = r"E:\CC\luan\test"#arcpy.env.scratchFolder##

    try:
        orderInfo = Oracle('prod').call_function('getorderinfo',orderID)
        OrderNumText = str(orderInfo["ORDER_NUM"])
        job_folder = os.path.join(MasterNAPL.job_prod_path,OrderNumText)
        jobGDB =os.path.join(job_folder,r"scratch.gdb")
        bufferGeometrySHP = os.path.join(jobGDB,r'bufferGeometry')


        outputlist = Oracle('prod').call_function('getselectedlist',orderID)
        worklist = []
        for item in outputlist:
            image_name  = item['IMAGE_NAME']
            newnapl = True if item['SOURCE']=='NAPL' else False
            georeferenced = True if item['GEOREFERENCED']=='Y' else False
            if newnapl:
                worklist.append([image_name,georeferenced])
        for [image_name,georeferenced] in worklist:
            [rollnum,photonum] = image_name[:-4].split("_")[:2]

            output = copyFoottoWork(rollnum,photonum,georeferenced,bufferGeometrySHP)
            arcpy.AddMessage("find to-add footprint: %s"%output)
        end = timeit.default_timer()
        arcpy.AddMessage(('done', round(end -start1,4)))
        arcpy.SetParameterAsText(1,worklist)

    except:
        import traceback
        tb = sys.exc_info()[2]
        tbinfo = traceback.format_tb(tb)[0]
        pymsg = "PYTHON ERRORS:\nTraceback info:\n %s \nError Info:\n %s"%(tbinfo,str(sys.exc_info()[1]))
        msgs = "ArcPy ERRORS:\n %s\n"%arcpy.GetMessages(2)
        arcpy.AddError("hit CC's error code in except: OrderID %s"%orderID)
        arcpy.AddError(pymsg)
        arcpy.AddError(msgs)
        raise

