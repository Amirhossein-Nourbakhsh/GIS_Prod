#-------------------------------------------------------------------------------
# Name:        module1
# Purpose:
#
# Author:      cchen
#
# Created:     10/05/2019
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

class Machine:
    machine_test = r"\\cabcvan1gis006"
    machine_prod = r"\\cabcvan1gis007"
class Credential:
    oracle_test = r"ERIS_GIS/gis295@GMTESTC.glaciermedia.inc"
    oracle_production = r"ERIS_GIS/gis295@GMPRODC.glaciermedia.inc"
class LAYER():
    def __init__(self,machine_path):
        self.machine_path = machine_path
        self.get()
    def get(self):
        machine_path = self.machine_path
        self.npllayer = os.path.join(machine_path,r"gptools\NHDReport",r"layer",r"NPL Boundaries_true.lyr")
class Oracle:
    # static variable: oracle_functions
    oracle_functions = {
    'getorderinfo':"eris_gis.getOrderInfo",
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
    def __init__(self,code):
        machine_path=self.machine_path
        self.LAYER=LAYER(machine_path)
class ProdConfig:
    machine_path=Machine.machine_prod
    def __init__(self,code):
        machine_path=self.machine_path
        self.LAYER=LAYER(machine_path)
def createOrderGeometry(pntCoords,output_folder, output_name, spatialRef = arcpy.SpatialReference(4269)):
    outputSHP=os.path.join(output_folder,output_name)
    arcpy.CreateFeatureclass_management(output_folder, output_name, "POLYGON", "", "DISABLED", "DISABLED", spatialRef)
    cursor = arcpy.da.InsertCursor(outputSHP, ['SHAPE@'])
    cursor.insertRow([arcpy.Polygon(arcpy.Array([arcpy.Point(*coords) for coords in pntCoords]),spatialRef)])
    del cursor
    return outputSHP

if __name__ == '__main__':
    try:
        ##  INPUT ##################
        orderID = arcpy.GetParameterAsText(0).strip()
        scratch = arcpy.env.scratchFolder

        ## PARAMETERS####################
        config = ProdConfig(9093)
        nplid=[]
        buffer_name = r'orderGeometry.shp'
        ####################################
         # 1  get order info by Oracle call
        orderInfo= Oracle('prod').call_function('getorderinfo',orderID)
        map_name = 'map_%s.jpg'%orderInfo[u'ORDER_NUM']
        ## STEPS ##########################
        # 2 create buffer geometry
        max_buffer_coords = eval(orderInfo[ u'BUFFER_GEOMETRY'][-1].values()[0])[0]
        buffer_nameSHP= createOrderGeometry(max_buffer_coords,scratch,buffer_name)
        end = timeit.default_timer()
        arcpy.AddMessage( ('create geometry shp', round(end -start1,4)))
        start=end

        # 3 select NPL
        NPL_groupLYR = arcpy.mapping.ListLayers(arcpy.mapping.Layer(config.LAYER.npllayer))
        nplyr = NPL_groupLYR[9]
        arcpy.SelectLayerByLocation_management(nplyr,'intersect',buffer_nameSHP)
        if int((arcpy.GetCount_management(nplyr).getOutput(0))) !=0:
            rows = arcpy.da.SearchCursor(nplyr,['CERCLIS_ID_1','CERCLIS_ID_2','CERCLIS_ID_3'])
            for row in rows:
                nplid = [_ for _ in row if len(_) == max(len(t) for t in row)]
            del rows
        del nplyr
        nplid = ",".join(nplid)
        end = timeit.default_timer()
        arcpy.AddMessage(('found npl id', round(end -start,4)))
        arcpy.SetParameterAsText(1,nplid)

    except:
        tb = sys.exc_info()[2]
        tbinfo = traceback.format_tb(tb)[0]
        pymsg = "PYTHON ERRORS:\nTraceback info:\n %s \nError Info:\n %s"%(tbinfo,str(sys.exc_info()[1]))
        msgs = "ArcPy ERRORS:\n %s\n"%arcpy.GetMessages(2)
        arcpy.AddError("hit CC's error code in except: Order ID:%s"%orderID)
        arcpy.AddError(pymsg)
        arcpy.AddError(msgs)
        raise

