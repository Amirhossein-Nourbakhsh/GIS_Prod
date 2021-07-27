#-------------------------------------------------------------------------------
# Name:        module1
# Purpose:     Georeferecing the input images
# Author:      hkiavarz
# Created:     15/03/2020
#-------------------------------------------------------------------------------
import arcpy, os ,timeit
import cx_Oracle
import json
from difflib import SequenceMatcher


class Server:
    server_test = r"\\cabcvan1gis006"
    server_prod = r"\\cabcvan1gis007"
class Credential:
    oracle_test = r"ERIS_GIS/gis295@GMTESTC.glaciermedia.inc"
    oracle_production = r"ERIS_GIS/gis295@GMPRODC.glaciermedia.inc"
class ResultPath:
    caaerial_prod= r"\\CABCVAN1OBI007\ErisData\prod\aerial_ca"
    caaerial_test= r"\\CABCVAN1OBI007\ErisData\test\aerial_ca"
    caaerial_work = r"\\cabcvan1obi007\ErisData\Work_aerial"
    caaerial_work_footprint = r"\\cabcvan1obi007\ErisData\Work_aerial\footprint"
    pdfreport_test= r"\\cabcvan1obi002\ErisData\Reports\test\reportcheck\AerialsDigital"
    pdfreport_prod = r"\\cabcvan1obi002\ErisData\Reports\prod\reportcheck\AerialsDigital"
class TransformationType():
    POLYORDER0 = "POLYORDER0"
    POLYORDER1 = "POLYORDER1"
    POLYORDER2 = "POLYORDER2"
    POLYORDER3 = "POLYORDER3"
    SPLINE = "ADJUST SPLINE"
    PROJECTIVE = "PROJECTIVE "
class ResamplingType():
    NEAREST  = "NEAREST"
    BILINEAR = "BILINEAR"
    CUBIC = "CUBIC"
    MAJORITY = "MAJORITY"
class TestConfig:
    server_path = Server.server_test
    caaerial_path = ResultPath.caaerial_test
    pdfreport_aerial_path = ResultPath.pdfreport_test
    def __init__(self):
        server_path=self.server_path
class ProdConfig:
    server_path= Server.server_prod
    caaerial_path = ResultPath.caaerial_prod
    pdfreport_aerial_path = ResultPath.pdfreport_prod
    def __init__(self):
        server_path=self.server_path
class Oracle:
     # static variable: oracle_functions
     oracle_function= "eris_aerial_can.getgeoreferencinginfo"
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
     def call_function(self,function_name,orderID,imgName):
            self.connect_to_oracle()
            cursor = self.cursor
            try:
                outType = cx_Oracle.CLOB
                inputParameters = [orderID,imgName +'.tif']
                output = json.loads(cursor.callfunc(function_name,outType,inputParameters).read())
                return output
            except cx_Oracle.Error as e:
                raise Exception(("Oracle Failure",e.message.message))
            except Exception as e:
                raise Exception(("JSON Failure",e.message))
            except NameError as e:
                raise Exception("Bad Function")
            finally:
               self.close_connection()
     def getOrderNum(self,orderID):
            self.connect_to_oracle()
            cursor = self.cursor
            try:
                outType = cx_Oracle.CLOB
                output = cursor.callfunc('eris_aerial_can.getOrderNum',outType,[orderID]).read()
                return output
            except cx_Oracle.Error as e:
                raise Exception(("Oracle Failure",e.message.message))
            except Exception as e:
                raise Exception(("JSON Failure",e.message))
            except NameError as e:
                raise Exception("Bad Function")
            finally:
               self.close_connection()
class GeoReferecing:
    gcpFile = ""
    srcPoints =""
    gcpPoints = ""
    inputRaster = ""
    outpath = ""
    outputRaster = ""
    transType = ""
    resType = ""
    def Apply(self,tempGDB,inputRaster,srcPoints,gcpPoints,transType, resType):
        arcpy.AddMessage('Start Georeferencing...')
        out_coor_system = arcpy.SpatialReference(4326)

        # georeference to WGS84
        gcsImage_wgs84 = arcpy.Warp_management(inputRaster, srcPoints,gcpPoints,os.path.join(tempGDB,'image_gc'), transType, resType)

        # Define projection system for output image after warpping the raw image
        arcpy.DefineProjection_management(gcsImage_wgs84, out_coor_system)
        arcpy.AddMessage('--Georeferencing Done.')
        return gcsImage_wgs84
def ClipbyGeometry(envSetting,tempGDB, inputImg, coordinates):
    arcpy.AddMessage('Start Clipping...')
    spatialRef = arcpy.SpatialReference(4326)

    # Create temp polygon(clipper) featureclass -- > Envelope
    clp_FC = arcpy.CreateFeatureclass_management(tempGDB,"clipper", "POLYGON", "", "DISABLED", "DISABLED", spatialRef)
    cursor = arcpy.da.InsertCursor(clp_FC, ['SHAPE@'])
    cursor.insertRow([arcpy.Polygon(arcpy.Array([arcpy.Point(*coords) for coords in coordinates]),spatialRef)])
    del cursor
    # Clip the georeferenced image
    outpuImg = arcpy.Clip_management(inputImg,"",os.path.join(tempGDB,'image_clp'),clp_FC,"256","ClippingGeometry", "NO_MAINTAIN_EXTENT")
    arcpy.AddMessage('-- Clipping Done.')
    return outpuImg
def ExportToJobFolder(envSetting,inputRaster,coordinates):
    arcpy.AddMessage('Start Exporting...')
    
    arcpy.CopyRaster_management(inputRaster, envSetting.ouputImage_org, "DEFAULTS","256","256","","","16_BIT_UNSIGNED") # Export to geo-tif
    arcpy.AddMessage(envSetting.ouputImage_org)
    
    arcpy.CopyRaster_management(inputRaster, envSetting.ouputImage_Work, "DEFAULTS","256","256","","","16_BIT_UNSIGNED") # Export to geo-tif
    arcpy.AddMessage(envSetting.ouputImage_Work)
    
    arcpy.AddMessage('Exporting Done.')
def CoordToString(inputObj):
    coordPts_string = ""
    for i in range(len(inputObj)-1):
            coordPts_string +=  "'" + " ".join(str(i) for i in  inputObj[i]) + "';"
    result =  coordPts_string[:-1]
    return result
class Env_Setting:
    #### ENVIRONMENTAL SETTING ####
    imgName = ""
    orderID = ""
    orderNum = ""
    scrachPath = ""
    inputImage = ""
    ouputImage_org = ""
    ouputImage_Work = ""
    ouputFootprintGDB = ""
    def __init__(self,env,inputOrderNum,inputImgName):
        self.orderNum = inputOrderNum
        self.imgName = inputImgName
        self.scrachPath = arcpy.env.scratchFolder
        if env == 'test':
            self.inputImage = os.path.join('r',ResultPath.caaerial_test,str(self.orderNum),'gc',self.imgName + '_g.png')
            self.ouputImage_org =  os.path.join('r',ResultPath.caaerial_test,str(self.orderNum),'org',self.imgName +'_gc.tif')
        elif env == 'prod':
            self.inputImage = os.path.join('r',ResultPath.caaerial_prod,str(self.orderNum),'gc',self.imgName + '_g.png')
            self.ouputImage_org =  os.path.join('r',ResultPath.caaerial_prod,str(self.orderNum),'org',self.imgName +'_gc.tif')
        self.ouputImage_Work = os.path.join('r',ResultPath.caaerial_work,'georeferenced', self.imgName + "_gc.tif")
        self.ouputFootprintGDB = os.path.join('r',ResultPath.caaerial_work_footprint,'fin_footprint.gdb')
if __name__ == '__main__':
    start1 = timeit.default_timer()
    ### set input parameters
    orderID = arcpy.GetParameterAsText(0)
    orderNum = arcpy.GetParameterAsText(1)
    imgName = arcpy.GetParameterAsText(2)
    env = arcpy.GetParameterAsText(3)

##    orderID = "867903" # temp
##    orderNum = '20200604014'
##    imgName = "A16531_42_gc.tif" # temp
##    ##aerial_info = '{"footPrint" : [[-75.62965595318006, 45.44526017690442], [-75.65959752515505, 45.44647765719718], [-75.66134061852685, 45.42537351455413], [-75.63139904655186, 45.424155578861935], [-75.62965595318006, 45.44526017690442]],  "envelope" : [[-75.629656, 45.4464777], [-75.6613406, 45.4464777], [-75.6613406, 45.4241556], [-75.629656, 45.4241556], [-75.629656, 45.4464777]],"rotation" : 16.9082,"imgname" :"A27696_36.tif","imgurl" : "https://erisservice7.ecologeris.com/erisdata/test/aerial_ca/20200302008/jpg/A27696_36.tif.jpg","ctrpts" : {"src" : [[-368.4813232421874, 553.2304687499999], [518.3145751953126, 214.2412109374999], [-178.6473388671874, -132.8837890625001]],"srcproj" : "EPSG:3857","dst" : [[-113.57867227795408, 53.57078733186189], [-113.55635629895018, 53.549069061876914], [-113.59154688122557, 53.534379970226055]],"dstproj" : "EPSG:4326"}}'
##    env = "test" # temp
    try:
        ### get georef information
        if imgName.find('_gc.tif') > 0:
            imgName = imgName.split('.tif')[0]
            inputGeorefInfo_json = Oracle(env).call_function('eris_aerial_can.getgeoreferencinginfo',orderID, imgName)
        elif imgName.find('.tif') > 0 :
            imgName = imgName.split('.tif')[0]
            inputGeorefInfo_json = Oracle(env).call_function('eris_aerial_can.getgeoreferencinginfo',orderID, imgName)
        imgName = imgName.split('_')[0] + '_' + imgName.split('_')[1]
        #inputGeorefInfo_json = Oracle(env).call_function('eris_aerial_can.getgeoreferencinginfo',orderID, imgName)
        ### instantiat Environment setting object
        envSetting = Env_Setting(env,orderNum,imgName)
        ### Create temp gdb CreatePersonalGDB
        tempGDB =os.path.join(envSetting.scrachPath,r"temp.gdb")

        if not os.path.exists(tempGDB):
            arcpy.CreateFileGDB_management(envSetting.scrachPath,r"temp.gdb")
        arcpy.env.workspace = tempGDB
        arcpy.env.overwriteOutput = True

	### assign gp class's properties values
        gp = GeoReferecing()
        gp.inputRaster = envSetting.inputImage
        gp.transType = TransformationType.POLYORDER1
        gp.resType = ResamplingType.BILINEAR
        gp.gcpPoints = CoordToString(inputGeorefInfo_json['envelope'])

	### Source point from input extent

        TOP = str(arcpy.GetRasterProperties_management(gp.inputRaster,"TOP").getOutput(0))
        LEFT = str(arcpy.GetRasterProperties_management(gp.inputRaster,"LEFT").getOutput(0))
        RIGHT = str(arcpy.GetRasterProperties_management(gp.inputRaster,"RIGHT").getOutput(0))
        BOTTOM = str(arcpy.GetRasterProperties_management(gp.inputRaster,"BOTTOM").getOutput(0))
        gp.srcPoints = "'" + LEFT + " " + BOTTOM + "';" + "'" + RIGHT + " " + BOTTOM + "';" + "'" + RIGHT + " " + TOP + "';" + "'" + LEFT + " " + TOP + "'"

	### Apply Georefrencing upon gcp points on input raw image
        img_Georeferenced = gp.Apply(tempGDB, gp.inputRaster, gp.srcPoints,gp.gcpPoints, gp.transType, gp.resType)

        ### Clip georeferenced image by footprint as clipper polygon"
        img_clp = ClipbyGeometry(envSetting,tempGDB,img_Georeferenced, inputGeorefInfo_json['footPrint'])

        ### Export
        ExportToJobFolder(envSetting,img_clp,inputGeorefInfo_json['footPrint'])

        ### Delete temp geodatabse
        if os.path.exists(tempGDB):
            arcpy.Delete_management(tempGDB)

        end = timeit.default_timer()
        arcpy.AddMessage(('Duration:', round(end -start1,4)))

    except:
        msgs = "ArcPy ERRORS:\n %s\n"%arcpy.GetMessages(2)
        arcpy.AddError(msgs)
        raise
