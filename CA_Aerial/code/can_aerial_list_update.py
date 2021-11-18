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
import shutil
class Credential:
    oracle_test = r"ERIS_GIS/gis295@GMTESTC.glaciermedia.inc"
    oracle_production = r"ERIS_GIS/gis295@GMPRODC.glaciermedia.inc"
class Oracle:

    # static variable: oracle_functions
    oracle_functions = {'getorderinfo':"eris_gis.getOrderInfo",
    'setaeriallistcan':'eris_aerial_can.setAerialListCan',
    'setdpi':'eris_aerial_can.setDpi'
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
class JobPath:
    job_folder_test = r"\\CABCVAN1OBI007\ErisData\test\aerial_ca"
    job_folder_prod = r"\\CABCVAN1OBI007\ErisData\prod\aerial_ca"
class Aerial:
    def __init__(self,ID,OrderNum):
        self.template_insertImage={
            "ORDER_ID" : int(ID),
            "ORDER_NUM":OrderNum,
            "IMAGE_NAME" : ""}

def copyImgtoWork(order_id,order_num,img_name,georef):
    templist = []
    job_folder =os.path.join(JobPath.job_folder_prod,order_num)
    start = timeit.default_timer()
    jobGDB =os.path.join(job_folder,r"scratch.gdb")
    if not os.path.exists(jobGDB):
        arcpy.CreateFileGDB_management(job_folder,r"scratch.gdb")
    init_folder = os.path.join(job_folder,'org')
    if not os.path.exists(init_folder):
        os.makedirs(init_folder)


    if georef=='Y':
        if img_name[-7:]!='_gc.tif':
            img_name =img_name.replace(".tif","_gc.tif")
        if os.path.exists(os.path.join(georeference_folder,img_name)):
            arcpy.CopyRaster_management(os.path.join(georeference_folder,img_name),os.path.join(job_folder,'org',img_name),"DEFAULTS","0","9")
            arcpy.Copy_management(os.path.join(georeference_folder,img_name),os.path.join(job_folder,'org',img_name.replace(".tif","_r.tif")))
        elif os.path.exists(os.path.join(work_folder_geo,img_name)):
            arcpy.CopyRaster_management(os.path.join(work_folder_geo,img_name),os.path.join(job_folder,'org',img_name),"DEFAULTS","0","9")
            arcpy.Copy_management(os.path.join(work_folder_geo,img_name),os.path.join(job_folder,'org',img_name.replace(".tif","_r.tif")))
        else:
            arcpy.AddMessage("georeferenced image not found:  %s"%img_name)
            return [img_name,templist]
        templist.append([os.path.join(job_folder,'org',img_name),os.path.join(job_folder,'org',img_name.replace(".tif","_r.tif"))])
        end = timeit.default_timer()
        arcpy.AddMessage(('copy inhouse aerial: %s"'%img_name, round(end -start,4)))
        start = end

    elif georef !='Y':
        if os.path.exists(os.path.join(work_folder_nongeo,img_name)):
            arcpy.AddMessage('exit inhouse nongeo aerial: %s"'%img_name)
            arcpy.Copy_management(os.path.join(work_folder_nongeo,img_name),os.path.join(job_folder,'org',img_name))#,"DEFAULTS","0","9")
        elif os.path.exists(os.path.join(nongeoreference_folder,img_name)):
            arcpy.Copy_management(os.path.join(nongeoreference_folder,img_name),os.path.join(job_folder,'org',img_name))#,"DEFAULTS","0","9")
        else:
            arcpy.AddMessage("nongeoreferenced image not found:  %s"%img_name)
            return [img_name,templist]
        end = timeit.default_timer()
        arcpy.AddMessage(('copy inhouse nongeo aerial: %s"'%img_name, round(end -start,4)))
        start = end
    dpi = checkDPI(os.path.join(job_folder,'org',img_name))
    arcpy.AddMessage('dpi inhouse aerial: %s"'%dpi)
    if dpi !=0:
        Oracle('prod').pass_values('setdpi',[img_name,dpi])
    return [img_name,templist]

def copyImgtoInhouse(img_name):
    if os.path.exists(os.path.join(work_folder_geo,img_name)):
        if os.path.exists(os.path.join(georeference_folder,img_name)):
            from datetime import date
            timestamp = '%s_%s_%s_'%(tuple(date.today().timetuple())[:3])
            arcpy.Delete_management(os.path.join(georeference_folder,'bk',timestamp+img_name))
            arcpy.Copy_management(os.path.join(georeference_folder,img_name),os.path.join(georeference_folder,'bk',timestamp+img_name))
            arcpy.Delete_management(os.path.join(georeference_folder,img_name))
        shutil.copy(os.path.join(work_folder_geo,img_name),os.path.join(georeference_folder,img_name))
        status = 'Done'
    else:
        arcpy.AddError("image is not found or georeferenced or image name is bad: %s"%img_name)
        status = 'Wrong'
    return status

def checkDPI(tifpath):
    from PIL import Image
    dpi =0
    try:
        im = Image.open(tifpath)
        if 'dpi' in im.info.keys():
            dpi =im.info['dpi'][0]
        elif 'resolution' in im.info.keys():
            dpi = im.info['resolution'][0]
        else:
            badfiles.append(tifpath)
        dpi = int(dpi)
        if int(dpi) != 300:
            if int(dpi)==1:
                dpi = 0
            elif dpi == 299L:
                dpi = 299
            dpi =int(dpi)
    except:
        arcpy.AddMessage( "Failed to open%s"%tifpath)
    return dpi

def getEnvenlope(foot):
    coords= []
    for row in arcpy.da.UpdateCursor(foot,['Shape@']):
        for array in row[0]:
            for pnt in array:
                if pnt:
                    coord = [round(pnt.X,7),round(pnt.Y,7)]
                    coords.append(coord)
            if len(coords) ==5:
                coords= str([coords]).replace(" ","")
                return coords
            else:
                return ''
def copyFoottoInhouse(img_name):
    [rollnum,photonum] = img_name[:-4].split("_")[:2]
    photonum = photonum.replace("_gc","")
    masterLayer_central = arcpy.mapping.Layer(master_napl_georeferenced)
    footprint = os.path.join(work_footprintGDB_fin,"%s_%s"%(rollnum,photonum))
    envelope = getEnvenlope(footprint)
    features = arcpy.SearchCursor(footprint)
    for feature in features:
        if len(arcpy.ListFields(footprint,"roll_num"))>0:
             rollnum = feature.roll_num
             photonum =feature.photo_num
        else:
            rollnum = feature.Roll if (feature.Roll !=rollnum) and (feature.Roll !=None) else rollnum#
            photonum = feature.Photo if (feature.Photo !=photonum) and (feature.Roll !=None) else photonum

        arcpy.SelectLayerByAttribute_management(masterLayer_central, "NEW_SELECTION", "roll_num = '" + rollnum + "' and photo_num ='" + photonum +"'")
        if int(arcpy.GetCount_management(masterLayer_central).getOutput(0)) == 0:   #new, need to insert
            rows = arcpy.InsertCursor(master_napl_georeferenced)
            row = rows.newRow()
            row.setValue("roll_num", rollnum)
            row.setValue("photo_num", photonum)
            row.setValue("Envelope_box", envelope)
            row.shape = feature.shape
            rows.insertRow(row)
            del row, rows

        elif int(arcpy.GetCount_management(masterLayer_central).getOutput(0)) == 1:   #to update
            rows = arcpy.UpdateCursor(master_napl_georeferenced, "roll_num = '"+rollnum + "' and photo_num = '" + photonum+"'")
            row = rows.next()
            row.shape = feature.shape
            row.Envelope_box =envelope
            rows.updateRow(row)
            del row, rows
        else:
            print "Duplicate reocrds in master shapefile: "+ 1
    del feature, features
    del masterLayer_central
    del footprint
    return envelope

if __name__ == '__main__':
    arcpy.env.overwriteOutput = 1
#    #### PATH SETTING ####
    start = timeit.default_timer()
    mainPath = r"\\cabcvan1gis007\gptools\Aerial_CAN\masterfile"
    nongeoreference_folder = r"\\cabcvan1gis001\NAPL\non_georeferenced"
    georeference_folder = r"\\cabcvan1gis001\NAPL\georeferenced"
    work_folder_nongeo = r"\\CABCVAN1OBI007\ErisData\Work_aerial\non_georeferenced"
    work_folder_geo = r"\\CABCVAN1OBI007\ErisData\Work_aerial\georeferenced"
    work_footprintGDB_orig = r"\\CABCVAN1OBI007\ErisData\Work_aerial\footprint\orig_footprint.gdb"
    work_footprintGDB_fin = r"\\CABCVAN1OBI007\ErisData\Work_aerial\footprint\fin_footprint.gdb"
    master_napl_all = r"\\cabcvan1gis007\gptools\Aerial_CAN\masterfile\napl.gdb\footprint_napl_all"
    master_napl_georeferenced = r"\\cabcvan1gis007\gptools\Aerial_CAN\masterfile\napl.gdb\footprint_napl_georeferenced"

#    #### ENVIRONMENTAL SETTING ####
    orderID ="823775"#arcpy.GetParameterAsText(0)#"742700" "767597"##
    imgname = 'A25156_187_gc.tif'#arcpy.GetParameterAsText(1).strip()#
    towork = "N"#arcpy.GetParameterAsText(2).strip().upper()#'Y'#
    georeferenced ='Y'#arcpy.GetParameterAsText(3).strip().upper()# 'Y'##
    scratch = r"F:\testing\scratch\code\scratch"#arcpy.env.scratchFolder# r"E:\CC\luan\test1"##

    try:
        # 1 create Order
        orderInfo = Oracle('prod').call_function('getorderinfo',orderID)
        OrderNumText = str(orderInfo["ORDER_NUM"])
        if towork =='Y':
            [imgname,gclist] = copyImgtoWork(orderID,OrderNumText,imgname,georeferenced)
            url = r"http://erisservice7.ecologeris.com/ErisInt/AerialPDF_prod/CAAerial.svc/ConvertSingleImage?o=%s&f=%s"%(OrderNumText,imgname)
            contextlib.closing(urllib.urlopen(url))
            if gclist !=[]:
                for [stfile,gcfile] in gclist:
                    os.remove(stfile)
                    os.rename(gcfile,stfile)
            output = imgname
        elif towork=='N' and georeferenced=='Y':
            url = r"http://erisservice7.ecologeris.com/ErisInt/AerialPDF_prod/CAAerial.svc/ConvertSingleImage?o=%s&f=%s"%(OrderNumText,imgname)
            contextlib.closing(urllib.urlopen(url))
            status = copyImgtoInhouse(imgname)
            output = copyFoottoInhouse(imgname)
            if output =='':
                arcpy.AddError("bad footprint: imgname %s"%imgname)
        else:
            arcpy.AddError("image is not georeferenced or image name is bad: %s"%imgname)
            output =''
        arcpy.SetParameterAsText(4,output)
        end = timeit.default_timer()
        arcpy.AddMessage(('done: %s, %s'%(imgname,output), round(end -start,4)))
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

