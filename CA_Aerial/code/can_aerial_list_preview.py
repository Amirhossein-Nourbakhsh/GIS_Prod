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
# -*- coding: utf-8 -*-
import os
import arcpy
import traceback
import sys
import shutil
import timeit
import cx_Oracle
import json
import contextlib
import urllib
import itertools

start1 = timeit.default_timer()
arcpy.env.overwriteOutput = True

eris_report_path = r"gptools\ERISReport"
eris_aerial_ca_path = r"gptools\Aerial_CAN"

class Machine:
    machine_test = r"\\cabcvan1gis006"
    machine_prod = r"\\cabcvan1gis007"
class Credential:
    oracle_test = r"ERIS_GIS/gis295@GMTESTC.glaciermedia.inc"
    oracle_production = r"ERIS_GIS/gis295@GMPRODC.glaciermedia.inc"
class ReportPath:
    caaerial_prod= r"\\CABCVAN1OBI007\ErisData\prod\aerial_ca"
    caaerial_test= r"\\CABCVAN1OBI007\ErisData\test\aerial_ca"
class TestConfig:
    machine_path=Machine.machine_test
    caaerial_path = ReportPath.caaerial_test

    def __init__(self):
        machine_path=self.machine_path
        self.LAYER=LAYER(machine_path)
        self.MXD=MXD(machine_path)
##        self.LOGO = LOGOFILE(machine_path,code)
class ProdConfig:
    machine_path=Machine.machine_prod
    caaerial_path = ReportPath.caaerial_prod

    def __init__(self):
        machine_path=self.machine_path
        self.LAYER=LAYER(machine_path)
        self.MXD=MXD(machine_path)
##        self.LOGO = LOGOFILE(machine_path,code)

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
    def turnOffLayer(self,position =0):
        layer = arcpy.mapping.ListLayers(self.mxd, "*", self.df)[position]
        layer.visible = False
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
        self.worldsatellite = os.path.join(machine_path,eris_aerial_ca_path,r"mxd","World_Imagery.lyr")
class MXD():
    def __init__(self,machine_path):
        self.machine_path = machine_path
        self.get()
    def get(self):
        machine_path = self.machine_path
        self.mxdaerial = os.path.join(machine_path,eris_aerial_ca_path,'mxd','Aerial_CA.mxd')

class Oracle:
    # static variable: oracle_functions
    oracle_functions = {'getorderinfo':"eris_gis.getOrderInfo",
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

class Aerial:
    def __init__(self,ID,OrderNum):
        self.template_insertImage={
            "ORDER_ID" : int(ID),
            "ORDER_NUM":OrderNum,
            "IMAGE_NAME" : "",
            "YEAR" : 0,
             "DECADE" : 0,
            "SELECTED" : "N",
            "IN_HOUSE" : "N",
            "GEOREFERENCED" : "N",
            "SCALE" : 0,
            "DPI" : 0,
            "FOOTPRINT_ID" : "",
            "FOOTPRINT_KMZ_NAME" : "",
            "NEW_FOOTPRINT":"",
            "ENVELOPE_BOX" : "",
            "SOURCE" : '',
            "NEW_IMAGE_NAME":""
        }

def createBuffers(orderBuffers,output_folder,buffer_name=r"buffer_%s"):
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

def getFourCorners(shpfile):
    coords=[]
    for row in arcpy.da.SearchCursor(shpfile,['Shape@']):
        for array in row[0]:
            for pnt in array:
                if pnt:
                    coords.append([round(pnt.X,7),round(pnt.Y,7)])
    if len(coords) ==5:
        return coords[:4]
    else:
        arcpy.AddMessage('%s Footprint needs to repair'%shpfile)
        xList = sorted([_[0] for _ in coords[:-1]])[:2]+sorted([_[0] for _ in coords[:-1]])[-2:]
        yList =sorted([_[1] for _ in coords[:-1]])[:2]+sorted([_[1] for _ in coords[:-1]])[-2:]
        newcoords = []
        badcoords = []
        for coord in coords[:-1]:
            if coord[0] not in xList or coord[1] not in yList:
                badcoords.append(coord)
            else:
                newcoords.append(coord)
                if len(newcoords)==4:
                    return newcoords
        if len(newcoords)<4:
            for coord in badcoords:
                newcoords.append(coord)
                if len(newcoords) ==4:
                    return newcoords

def checkAvailability(master_GDB,decadelist,site_geometry,field_names,spatial_relation, multipleImages):
    templist =[]
    metadata_list = []
    temp_decadelist = [_ for _ in decadelist]
    decades = range(1920,2020,10)
    arcpy.env.workspace = master_GDB
    available_year_dict={}
    selected_year_dict = {}
    bestYear_in_Decades={}

    for masterfile in arcpy.ListFeatureClasses():
        masterfile = arcpy.mapping.Layer(masterfile)
        arcpy.SelectLayerByLocation_management(masterfile,spatial_relation,site_geometry)
        if int(arcpy.GetCount_management(masterfile)[0])!=0:
            rows = arcpy.SearchCursor(masterfile)
            for row in rows:
                try:
                    try:
                        year = int(row.YEAR)
                    except RuntimeError:
                        year = int(row.Year)
                except:
                    break
                decade = [_ for _ in decades if year>=_][-1]
                if decade in temp_decadelist:
                    row_string = "row.".join(field_names.split(" "))
                    metadata_list.append([decade,year,eval("[%s]"%row_string)])
                    if multipleImages !='Y':
                        temp_decadelist.remove(decade)
                    templist.append(decade)
            del row
            del rows
            available_year_dict[masterfile.name] =[_[:2] for _ in metadata_list]

    for mapservice in available_year_dict.keys():
        for [decade, year] in available_year_dict[mapservice]:
            if decade not in bestYear_in_Decades.keys():
                bestYear_in_Decades[decade] =[year]
            else:
                temp = bestYear_in_Decades[decade]
                temp.append(year)
                bestYear_in_Decades[decade] = temp
        if bestYear_in_Decades !={}:
            firstYear = bestYear_in_Decades[sorted(bestYear_in_Decades.keys(),reverse=False)[0]][0]
            arcpy.AddMessage(('first year', firstYear))
        else:
            arcpy.AddMessage("not available")
        finalyeartemp = []
        for decade in bestYear_in_Decades.keys()[1:]:
            yearlist = bestYear_in_Decades[decade]
            if [_ for _ in yearlist if _ - firstYear>=5] !=[]:
                year =[_ for _ in yearlist if _ - firstYear>=5][0]
            else:
                year = yearlist[-1]
            finalyeartemp.append(year)
        selected_year_dict[mapservice] =finalyeartemp

    return [metadata_list,[_ for _ in decadelist if _ not in templist]]

def remove_overlap(input_data,output_data,threshold):
    where_clause =""
    removelist=[]
    count = 0
    data = sorted(arcpy.da.SearchCursor(input_data, ['Scale','OID@', 'SHAPE@']),reverse =True)[:100]
    cursor_area_ascending = sorted(data,reverse =True)
    cursor_area_descending = sorted(data,reverse =False)
    for e1,e2 in itertools.product(cursor_area_ascending, cursor_area_descending):
        if e1[2].overlaps(e2[2]):
            percentage = round(100*e1[2].intersect(e2[2],4).area/e1[2].area,2)
            if percentage>=threshold and e1[1] not in removelist:
                removelist.append(e2[1])
    removelist = set(removelist)
    elist = [_[1] for _ in cursor_area_ascending if _[1] not in removelist]
    for e in elist:
        where_clause ="OBJECTID= "+ str(e)+ " or "+where_clause
    del cursor_area_descending,cursor_area_ascending
    meta1 = arcpy.mapping.Layer(input_data)
    arcpy.SelectLayerByAttribute_management(meta1,"NEW_SELECTION",where_clause[:-4]+" or Georeferenced ='Y' or In_House ='Y' and Scale <=20000")
    arcpy.CopyFeatures_management(meta1,output_data)
    return 'removed %s overlapping records'%count

def createJSON(filefolder,filename,outputpath):
    outputfile =os.path.join(outputpath,filename+'.json')
    if "_nodata" not in filename:
        inputfile = os.path.join(filefolder,filename)
        keep_fields =[]
        for field in arcpy.ListFields(inputfile):
            keep_fields.append(field.name)
        with open(outputfile, 'a') as the_file:
            rows = arcpy.da.SearchCursor(inputfile,['Shape@']+keep_fields)
            for row in rows:
                templist = []
                for array in row[0]:
                    for pnt in array:
                        if pnt:
                            templist.append([round(pnt.X,10),round(pnt.Y,10)])
                        else:
                            raise Exception('ring')
                line1 = '{"geometry": {"type": "Polygon", "coordinates": %s},'%[templist]
                line2 = '"type": "Feature",'
                for i in range(len(keep_fields)):
                    line2 =line2+'"%s": "%s",'%(keep_fields[i],row[i+1])
                line = line1+ line2[:-1]+'}'
                the_file.write('%s\n'%line)
        the_file.close()
    else:
        with open(outputfile, 'a') as the_file:
            print ' no'
        the_file.close()
    return outputfile

def getWorldAerialYear((centroid_X,centroid_Y)):
    import re
    fsURL = r"https://services.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/0/query?f=json&returnGeometry=false&spatialRel=esriSpatialRelIntersects&maxAllowableOffset=0&geometryType=esriGeometryPoint&inSR=4326&outFields=SRC_DATE%2CNICE_DESC"
    params = urllib.urlencode({'geometry':"'geometry':'x':%s,'y':%s"%(centroid_X,centroid_Y)})
    resultBing = json.loads(urllib.urlopen(fsURL,params).read())
    if "error" not in str(resultBing) and '"features":[]' not in str(resultBing):
        year = str(resultBing['features'][0]['attributes']['SRC_DATE'])[0:4]
        source = str(resultBing['features'][0]['attributes']['NICE_DESC'])
    else:
        tries = 5
        key = False
        while tries >= 0:
            if "error" not in resultBing and '"features":[]' not in resultBing:
                year = str(resultBing['features'][0]['attributes']['SRC_DATE'])[0:4]
                source = str(resultBing['features'][0]['attributes']['NICE_DESC'])
            elif tries == 0:
                    return ["2015","DigitalGlobe"]
            else:
                time.sleep(5)
                tries -= 1
    return[year,source]

# ### ENVIRONMENTAL SETTING ####
arcpy.Delete_management(r"in_memory")
arcpy.env.overwriteOutput = True


if __name__ == '__main__':

#    #### PATH SETTING ####
    start = timeit.default_timer()
    mainPath = r"\\cabcvan1gis007\gptools\Aerial_CAN"
    napl_gdb = r"\\cabcvan1gis007\gptools\Aerial_CAN\masterfile\napl.gdb"
    map_ontario =r"\\cabcvan1gis007\gptools\Aerial_CAN\masterfile\ontario_map.gdb"
    img_ontario =r"\\cabcvan1gis007\gptools\Aerial_CAN\masterfile\ontario_img.gdb"
    nongeoreference_folder = r"\\cabcvan1fpr009\NAPL\non_georeferenced"
    georeference_folder = r"\\cabcvan1fpr009\NAPL\georeferenced"
    work_footprintGDB = r"\\CABCVAN1OBI007\ErisData\Work_aerial\footprint\orig_footprint.gdb"
    napl_all = os.path.join(napl_gdb,"footprint_napl_all")
    napl_georeferenced = os.path.join(napl_gdb,"footprint_napl_georeferenced")

#    #### ENVIRONMENTAL SETTING ####
    orderID = "1108464"#arcpy.GetParameterAsText(0)#"742700"
    isInit = 'Y'#arcpy.GetParameterAsText(1)#
    multi_images = 'Y'#arcpy.GetParameterAsText(2)#
    scratch = r"C:\Users\JLoucks\Documents\JL\test1"#arcpy.env.scratchFolder##
    scratch_mxd = os.path.join(scratch,"mxd_template.mxd")
    try:
        # 1 create Order
        orderInfo = Oracle('prod').call_function('getorderinfo',orderID)
        # 2 get decade list
        decade_ordered = [int(_) for _ in Oracle('prod').call_function('getorderdecades',orderID)]

        if decade_ordered ==[]:
            arcpy.AddMessage("empty decades")
            arcpy.SetParameterAsText(3,"empty decades")
            raise SystemExit

        OrderNumText = str(orderInfo["ORDER_NUM"])
        ## create Job Folder
        init_folder = os.path.join(scratch,OrderNumText,'org')

        if not os.path.exists(init_folder):
            os.makedirs(init_folder)
        config=ProdConfig()
        job_folder = os.path.join(config.caaerial_path,OrderNumText)
        ## create NAPL GDB
        scratchGDB_napl =os.path.join(scratch,r"napl_all.gdb")
        if not os.path.exists(scratchGDB_napl):
            arcpy.CreateFileGDB_management(scratch,r"napl_all.gdb")
        ## create filtered NAPL GDB
        scratchGDB_napl_filtered =os.path.join(scratch,r"napl_filtered.gdb")
        if not os.path.exists(scratchGDB_napl_filtered):
            arcpy.CreateFileGDB_management(scratch,r"napl_filtered.gdb")
        ## create Temp GDB
        tempGDB =os.path.join(scratch,r"temp.gdb")
        if not os.path.exists(tempGDB):
            arcpy.CreateFileGDB_management(scratch,r"temp.gdb")
        ## copy Georeferenced Master Footprint
        scratch_master_napl_temp = os.path.join(tempGDB, "master_napl_temp")
        arcpy.CopyFeatures_management(napl_georeferenced,scratch_master_napl_temp)
        where_clause = "Shape_Area <>0"
        meta = arcpy.mapping.Layer(scratch_master_napl_temp)
        arcpy.SelectLayerByAttribute_management(meta,"NEW_SELECTION",where_clause)
        scratch_master_napl= os.path.join(tempGDB, "master_napl")
        arcpy.CopyFeatures_management(meta,scratch_master_napl)
        del meta
        ############################################### MAIN ##################################################
        # Initial Checking
        if isInit == 'Y':
            ## create Scratch GDB
            scratchGDB =os.path.join(scratch,r"scratch.gdb")
            if not os.path.exists(scratchGDB):
                arcpy.CreateFileGDB_management(scratch,r"scratch.gdb")
            ## copy IMG GDB
            scratchGDB_img = os.path.join(scratch, "scratch_img.gdb")
            arcpy.Copy_management(img_ontario,scratchGDB_img)
            ## copy MAP GDB
            scratchGDB_map = os.path.join(scratch, "scratch_map.gdb")
            arcpy.Copy_management(map_ontario,scratchGDB_map)

            #    #### PARAMETER SETTING ####
            default_scale = 20000
            orderGeometry = r'orderGeometry'
            orderGeometrySHP = os.path.join(scratchGDB,orderGeometry)
            bufferGeometry = r'bufferGeometry'
            bufferGeometrySHP = os.path.join(scratchGDB,bufferGeometry)

            ## create Order Geometry
            orderGeometrySHP =  createGeometry(eval(orderInfo[u'ORDER_GEOMETRY'][u'GEOMETRY'])[0],orderInfo[u'ORDER_GEOMETRY'][u'GEOMETRY_TYPE'],scratchGDB,orderGeometry)
            arcpy.Buffer_analysis(orderGeometrySHP,bufferGeometrySHP,"0.5 KILOMETERS")

            # 3 get Map template ready

            arcpy.Copy_management(config.MXD.mxdaerial,scratch_mxd)

            map1 =Map(scratch_mxd)
            map1.addTextoMap('Address',"%s, %s"%(orderInfo['ADDRESS'],orderInfo['PROVSTATE']))
            map1.addTextoMap("OrderNum","Order Number: %s"%orderInfo['ORDER_NUM'])
            map1.df.spatialReference = arcpy.SpatialReference(3395)
            addOrderGeometry(map1,orderInfo[u'ORDER_GEOMETRY'][u'GEOMETRY_TYPE'],scratchGDB,orderGeometry)
            map1.turnOffLayer(0)

            # add World Imagery
            map1.df.spatialReference = arcpy.SpatialReference(4326)
            print 'running imagery year -----------------------------'
            [year,source] = getWorldAerialYear(eval(orderInfo['ORDER_GEOMETRY']['CENTROID'].strip('[]')))
            print str(year)
            print str(source)
            image = 'satellite_%s_%s.jpg'%(year,orderID)
            map1.addLayer(config.LAYER.worldsatellite,add_position="BOTTOM")
            map1.zoomToTopLayer()
            map1.toScale(default_scale*1.3) if map1.df.scale <= default_scale else map1.toScale(map1.df.scale*1.1)
            arcpy.mapping.ExportToJPEG(map1.mxd, os.path.join(init_folder,image), map1.df,df_export_width= 2550,df_export_height=3300,world_file = True, jpeg_quality=85)#,resolution = 300)#df_export_width= 14290,df_export_height=16000, color_mode='8-BIT_GRAYSCALE',world_file = True, jpeg_quality=100)
            insert_image = Aerial(orderID,OrderNumText).template_insertImage

            insert_image["SOURCE"] =source.decode('utf-8')
            insert_image["IMAGE_NAME"] = str(image)
            insert_image["NEW_IMAGE_NAME"] = str(image)+".jpg"
            insert_image["YEAR"] =year
            insert_image["DECADE"] = [_ for _ in range(1920,2020,10) if year>=_][-1]
            insert_image["IN_HOUSE"] = 'Y'
            insert_image["GEOREFERENCED"] = 'Y'
            insert_image["SCALE"] = default_scale*1.3
            insert_image["DPI"] = 300
            [lower_y,lower_x,upper_y,upper_x] = [round(map1.df.extent.YMin,7),round(map1.df.extent.XMin,7),round(map1.df.extent.YMax,7),round(map1.df.extent.XMax,7)]
            insert_image["ENVELOPE_BOX"] = str([[[lower_x,lower_y],[lower_x,upper_y],[upper_x,upper_y],[upper_x,lower_y]]])
            Oracle('prod').pass_values('setaeriallistcan',[json.dumps(insert_image,ensure_ascii=False)])
            for lyr in arcpy.mapping.ListLayers(map1.mxd, "" ,map1.df)[1:]:
                arcpy.mapping.RemoveLayer(map1.df, lyr)

            # 2 Ontario check: 1.Trt 30 40 50 2 Map  3 NAPL inhouse 4 NAPL  ########
            metadata_list=[]

            if orderInfo['PROVSTATE'] ==r'ON':
                ################ Img File #######################################################
                [metadata_list1,decade_ordered1] = checkAvailability(scratchGDB_img,decade_ordered, bufferGeometrySHP," Name, filepath, filename, source","COMPLETELY_CONTAINS",multi_images)
                if metadata_list1 ==[]:
                    multi_images1='Y'
                    [metadata_list1,decade_ordered1] = checkAvailability(scratchGDB_img,decade_ordered, bufferGeometrySHP," Name, filepath, filename, source","INTERSECT",multi_images1)
                end = timeit.default_timer()
                arcpy.AddMessage((' checkAvailability Img', round(end -start,4)))
                start=end
                arcpy.AddMessage("Total: %s, found %s, not found %s from Img Collections"%(decade_ordered, [_ for _ in decade_ordered if _ not in decade_ordered1], decade_ordered1))

                for [decade, year, [imgname,filepath,filename,source]] in metadata_list1:
                    image = str("img_%s_%s"%(year,filename[:-4].replace(".",""))+".jpg")
                    filepath = filepath.replace(r"\\CABCVAN1GIS001",r"\\cabcvan1fpr009")
                    arcpy.Copy_management(filepath,os.path.join(init_folder,image))#,"DEFAULTS","0","9","","","8_BIT_UNSIGNED")
                    insert_image = Aerial(orderID,OrderNumText).template_insertImage
                    insert_image["SOURCE"] =str(source)
                    insert_image["IMAGE_NAME"] = str(image)
                    insert_image["NEW_IMAGE_NAME"] = str(image)+".jpg"
                    insert_image["YEAR"] =year
                    insert_image["DECADE"] = decade
                    insert_image["IN_HOUSE"] = 'Y'
                    insert_image["GEOREFERENCED"] = 'Y'
                    insert_image["SCALE"] = default_scale
                    insert_image["DPI"] = 300
                    meta_img = arcpy.mapping.Layer(os.path.join(scratchGDB_img,imgname))
                    where_clause = "filename = '%s' "%filename
                    arcpy.SelectLayerByAttribute_management(meta_img,"NEW_SELECTION",where_clause)
                    if int(arcpy.GetCount_management(meta_img)[0])!=0:
                        footprint_img = os.path.join(tempGDB,"foot_%s"%filename[:-4].replace(".",""))
                        arcpy.CopyFeatures_management(meta_img,footprint_img)
                        footprint_img_pr = footprint_img+r"_wgs"
                        arcpy.Project_management(footprint_img,footprint_img_pr,arcpy.SpatialReference(4326))
                        coorlist_img = getFourCorners(footprint_img_pr)
                        insert_image["ENVELOPE_BOX"] = str([coorlist_img])
                    del meta_img
                    Oracle('prod').pass_values('setaeriallistcan',[str(insert_image)])

                ##############  Map Service #####################################################

                [metadata_list2,decade_ordered2] = checkAvailability(scratchGDB_map,decade_ordered1, bufferGeometrySHP," Name, SERV_LINK, source","COMPLETELY_CONTAINS",multi_images)
                if metadata_list2 ==[]:
                    multi_images1='Y'
                    [metadata_list2,decade_ordered2] = checkAvailability(scratchGDB_map,decade_ordered1, bufferGeometrySHP," Name, SERV_LINK, source","INTERSECT",multi_images1)
                end = timeit.default_timer()
                arcpy.AddMessage((' checkAvailability MAP', round(end -start,4)))
                start=end
                arcpy.AddMessage("Total: %s, found %s, not found %s from Map Collections"%(decade_ordered, [_ for _ in decade_ordered1 if _ not in decade_ordered2], decade_ordered2))
                #decade_ordered =decade_ordered2

                map1.df.spatialReference = arcpy.SpatialReference(4326)
                for [decade, year, [mapname,maplink,source]] in metadata_list2:
                    layerlist = os.listdir(os.path.join(mainPath,"layer",mapname))
                    if [_ for _ in layerlist if str(year) in _] !=[]:
                        image = 'map_%s_%s.jpg'%(year,orderID)
                        selected_layer = os.path.join(mainPath,"layer",mapname,[_ for _ in layerlist if str(year) in _][0])
                        ##print 'add layer'
                        map1.addLayer(selected_layer,add_position="BOTTOM")
                        map1.zoomToTopLayer()
                        map1.toScale(default_scale) if map1.df.scale <= default_scale else map1.toScale(map1.df.scale*1.1)

                        arcpy.mapping.ExportToJPEG(map1.mxd, os.path.join(init_folder,image), map1.df,df_export_width= 2550,df_export_height=3300,world_file = True, jpeg_quality=75)#,resolution = 300)#df_export_width= 14290,df_export_height=16000, color_mode='8-BIT_GRAYSCALE',world_file = True, jpeg_quality=100)
                        #arcpy.mapping.ExportToPDF(map1.mxd,os.path.join(init_folder,'map_%s_%s.pdf'%(year,orderID)),resolution = 600)
                        end = timeit.default_timer()
                        arcpy.AddMessage((' ExportToJPEG', round(end -start,4)))
                        start=end
                        [lower_y,lower_x,upper_y,upper_x] = [round(map1.df.extent.YMin,7),round(map1.df.extent.XMin,7),round(map1.df.extent.YMax,7),round(map1.df.extent.XMax,7)]
                        #arcpy.DefineProjection_management(os.path.join(init_folder,image),arcpy.SpatialReference(4326))
                        insert_image = Aerial(orderID,OrderNumText).template_insertImage
                        insert_image["SOURCE"] =str(source)
                        insert_image["IMAGE_NAME"] = str(image)
                        insert_image["NEW_IMAGE_NAME"] = str(image)+".jpg"
                        insert_image["YEAR"] =year
                        insert_image["DECADE"] = decade

                        insert_image["IN_HOUSE"] = 'Y'
                        insert_image["GEOREFERENCED"] = 'Y'
                        insert_image["SCALE"] = default_scale
                        insert_image["DPI"] = 96
                        insert_image["ENVELOPE_BOX"] = str([[[lower_x,lower_y],[lower_x,upper_y],[upper_x,upper_y],[upper_x,lower_y]]])
                        Oracle('prod').pass_values('setaeriallistcan',[str(insert_image)])
                        print (image, insert_image["ENVELOPE_BOX"])
                        for lyr in arcpy.mapping.ListLayers(map1.mxd, "" ,map1.df)[1:]:
                            arcpy.mapping.RemoveLayer(map1.df, lyr)
                        #arcpy.RefreshTOC()
                        end = timeit.default_timer()
                        ##print (' Done One Image', round(end -start,4))
                        start=end

        ##############  NAPL  #####################################################
        # 3 Other Prov, 1 NAPL inhouse 2 NAPL
            gclist = []
            footprint_kmz_name = ''
            nonapl_decades = []
            # ### select all napl footprints plus georeferenced footprints
            meta = arcpy.mapping.Layer(napl_all)
            arcpy.SelectLayerByLocation_management(meta,"INTERSECT",bufferGeometrySHP)

            if int(arcpy.GetCount_management(meta)[0])!=0:
                footprint = os.path.join(tempGDB,"napl_no_pr")
                arcpy.CopyFeatures_management(meta,footprint)
                napl_pr = os.path.join(tempGDB,"footprint_napl_all")
                arcpy.Project_management(footprint,napl_pr,arcpy.SpatialReference(4326))
                del meta
            # ### split into each decade of all napl footprints
                meta = arcpy.mapping.Layer(napl_pr)
                #meta.transparency = 100
                for decade in range(1920,2020,10):
                    where_clause = " or Year=".join([" "]+["'%s'"%str(decade+i) for i in range(10)])[5:]
                    arcpy.SelectLayerByAttribute_management(meta,"NEW_SELECTION",where_clause)
                    if int(arcpy.GetCount_management(meta)[0])!=0:
                        arcpy.CopyFeatures_management(meta,os.path.join(scratchGDB_napl,"footprint_%s"%decade))
                        if decade in decade_ordered:
                            start = timeit.default_timer()
                            remove_overlap(os.path.join(scratchGDB_napl,"footprint_%s"%decade),os.path.join(scratchGDB_napl_filtered,"footprint_%s_filtered"%decade),60)
                            end = timeit.default_timer()
                            arcpy.AddMessage(('NAPL overlapping', round(end -start,4)))
                            start=end
                    else:
                        nonapl_decades.append(decade)
                        decade = '%s_nodata'%decade
                    createJSON(scratchGDB_napl,"footprint_%s"%decade,os.path.join(scratch,OrderNumText))
                del meta
            else:
                arcpy.AddMessage("no NAPL available")
                arcpy.SetParameterAsText(3,"no NAPL available")
                raise SystemExit
            start = timeit.default_timer()

            [metadata_list3,decade_ordered3] =checkAvailability(scratchGDB_napl_filtered,decade_ordered,orderGeometrySHP," Roll, Photo, Scale","COMPLETELY_CONTAINS",multi_images)
            if metadata_list3 ==[] or [_ for _ in decade_ordered3 if _ not in nonapl_decades] !=[]:
                multi_images1='Y'
                [metadata_list3,decade_ordered3] = checkAvailability(scratchGDB_napl_filtered,decade_ordered, orderGeometrySHP," Roll, Photo, Scale","INTERSECT",multi_images1)
            end = timeit.default_timer()
            arcpy.AddMessage(('NAPL checkAvailability', round(end -start,4)))
            arcpy.AddMessage( "Total: %s, found %s, not found %s from NAPL Collections"%(decade_ordered, [_ for _ in decade_ordered if _ not in decade_ordered3], decade_ordered3))
            start=end

            for [decade,year,[roll,photo,scale]] in metadata_list3:
                roll_photo ="%s_%s"%(roll,photo)
                insert_image = Aerial(orderID,OrderNumText).template_insertImage
                image_name = "%s.tif"%roll_photo
                image_name_gc = "%s_gc.tif"%roll_photo

                insert_image["IMAGE_NAME"] = str(image_name)
                insert_image["NEW_IMAGE_NAME"] = str(image_name)+".jpg"
                insert_image["YEAR"] =year
                insert_image["DECADE"] = decade
                insert_image["SOURCE"] = 'NAPL'
                insert_image["SCALE"] = int(scale)
                insert_image["FOOTPRINT_KMZ_NAME"] = str('%s.json'%decade)
                insert_image["DPI"] = 300
                insert_image["GEOREFERENCED"] = 'N'

                found_georeferened_image = 'N'
                found_inhouse_image = 'N'

                if os.path.exists(os.path.join(georeference_folder,image_name_gc)):
                    image_name = image_name_gc
                    insert_image["IMAGE_NAME"] = str(image_name)
                    insert_image["NEW_IMAGE_NAME"] = str(image_name)+".jpg"

                    try:
                        arcpy.CopyRaster_management(os.path.join(georeference_folder,image_name_gc),os.path.join(init_folder,image_name),"DEFAULTS","0","9","","","8_BIT_UNSIGNED")
                        arcpy.Copy_management(os.path.join(georeference_folder,image_name_gc),os.path.join(init_folder,image_name.replace(".tif","_r.tif")))
                        gclist.append([image_name,image_name.replace(".tif","_r.tif")])
                        found_georeferened_image = 'Y'
                        insert_image["IN_HOUSE"] = 'Y'
                        insert_image["GEOREFERENCED"] = 'Y'
                    except:
                        arcpy.AddMessage("Failed copy image over")
                elif os.path.exists(os.path.join(nongeoreference_folder,image_name)):
                    try:
                        arcpy.Copy_management(os.path.join(nongeoreference_folder,image_name),os.path.join(init_folder,image_name))#,"DEFAULTS","0","9")
                        insert_image["IN_HOUSE"] = 'Y'
                        found_inhouse_image='Y'
                    except:
                        arcpy.AddError("failed copy image over")
                    if found_inhouse_image ==found_georeferened_image:
                        arcpy.AddMessage("image georeferenced but not flagged")
                else:
                    needOrder ='Y'
                    insert_image["IN_HOUSE"] = 'N'

                end = timeit.default_timer()
                arcpy.AddMessage(('copy %s'%image_name, round(end -start,4)))
                start=end
                Oracle('prod').pass_values('setaeriallistcan',[str(insert_image)])
                end = timeit.default_timer()
                arcpy.AddMessage(('insert record: %s'%image_name, round(end -start,4)))
                start=end

            arcpy.Copy_management(scratchGDB,os.path.join(scratch,OrderNumText,"scratch.gdb"))
            if os.path.exists(job_folder):
                shutil.rmtree(job_folder,ignore_errors=True)
            shutil.copytree(os.path.join(scratch,OrderNumText), job_folder)
            end = timeit.default_timer()
            arcpy.AddMessage(('copy init folder to 0bi007', round(end -start,4)))
            start=end
            url = r"http://erisservice7.ecologeris.com/ErisInt/AerialPDF_prod/CAAerial.svc/ConvertImage?o=%s"%OrderNumText
            contextlib.closing(urllib.urlopen(url))
            end = timeit.default_timer()
            arcpy.AddMessage(('convert org folder to jpg', round(end -start,4)))
            start=end
            if gclist !=[]:
                for [stfile,gcfile] in gclist:
                    os.remove(os.path.join(job_folder,"org",stfile))
                    os.rename(os.path.join(job_folder,"org",gcfile),os.path.join(job_folder,"org",stfile))
            arcpy.AddMessage(('Complete ', round(start -start1,4)))
        arcpy.SetParameterAsText(3,job_folder)


    except:
        tb = sys.exc_info()[2]
        tbinfo = traceback.format_tb(tb)[0]
        pymsg = "PYTHON ERRORS:\nTraceback info:\n %s \nError Info:\n %s"%(tbinfo,str(sys.exc_info()[1]))
        msgs = "ArcPy ERRORS:\n %s\n"%arcpy.GetMessages(2)
        arcpy.AddError("hit CC's error code in except: OrderID %s"%orderID)
        arcpy.AddError(pymsg)
        arcpy.AddError(msgs)
        raise

