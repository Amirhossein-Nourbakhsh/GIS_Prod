#-------------------------------------------------------------------------------
# Name:        module1
# Purpose:
#
# Author:      cchen
#
# Created:     27/04/2017
# Copyright:   (c) cchen 2017
# Licence:     <your licence>
#-------------------------------------------------------------------------------
from collections import OrderedDict
import os
import ConfigParser

def server_loc_config(configpath,environment):
    configParser = ConfigParser.RawConfigParser()
    configParser.read(configpath)
    if environment == 'test':
        reportcheck = configParser.get('server-config','reportcheck_test')
        reportviewer = configParser.get('server-config','reportviewer_test')
        reportinstant = configParser.get('server-config','instant_test')
        reportnoninstant = configParser.get('server-config','noninstant_test')
        upload_viewer = configParser.get('url-config','uploadviewer')
        server_config = {'reportcheck':reportcheck,'viewer':reportviewer,'instant':reportinstant,'noninstant':reportnoninstant,'viewer_upload':upload_viewer}
        return server_config
    elif environment == 'prod':
        reportcheck = configParser.get('server-config','reportcheck_prod')
        reportviewer = configParser.get('server-config','reportviewer_prod')
        reportinstant = configParser.get('server-config','instant_prod')
        reportnoninstant = configParser.get('server-config','noninstant_prod')
        upload_viewer = configParser.get('url-config','uploadviewer_prod')
        server_config = {'reportcheck':reportcheck,'viewer':reportviewer,'instant':reportinstant,'noninstant':reportnoninstant,'viewer_upload':upload_viewer}
        return server_config
    else:
        return 'invalid server configuration'

# DS OID#
TOPA = '12906'
TOPO_ON = '12905'
TOPO_BC = '12903'
TOPO_NS = '12904'
TopoList = [TOPA,TOPO_BC,TOPO_NS,TOPO_ON]

SLC = '12886'
SOI3_ON = '12727'
SOIL_AB = '12595'
SOIL_BC = '12596'
SOIL_MB = '12732'
SOIL_NB = '12827'
SOIL_NL = '12731'
SOIL_NS = '12733'
SOIL_PE = '12762'
SOIL_QC = '12828'
SOIL_SK = '12734'
SOIL_YT = '12735'
SoilList = [SLC,SOI3_ON,SOIL_AB,SOIL_BC,SOIL_MB,SOIL_NB,SOIL_NL,SOIL_NS,SOIL_PE,SOIL_QC,SOIL_SK,SOIL_YT]

BGEC = '12913' #bedrock federal
BGEO_AB = '12911' # bedrock AB
BGEO_BC = '12917'
BGEO_MB = '12919'
BGEO_NB = '12921'
BGEO_NL = '12922'
BGEO_NS = '12924'
BGEO_ON = '12240'
BGEO_QC = '12927'
BGEO_SK = '12928'
BGEO_YT = '12930'
BedrockList = [BGEC,BGEO_AB,BGEO_BC,BGEO_MB,BGEO_NB,BGEO_NL,BGEO_NS,BGEO_ON,BGEO_QC,BGEO_SK,BGEO_YT]

SGEC = '12912' # surficial federal
SGEO_ON = '12242'
SGEO_AB = '12916'
SGEO_BC = '12918'
SGEO_MB = '12920'
SGEO_NL = '12923'
SGEO_NS = '12925'
SGEO_SK = '12929'
SGEO_YT = '12931'
SurficialList = [SGEC,SGEO_AB,SGEO_ON,SGEO_BC,SGEO_MB,SGEO_NL,SGEO_NS,SGEO_SK,SGEO_YT]

WTLD_AB = '12893'
WTLD_BC = '12894'
WTLD_MB = '12895'
WTLD_NB = '12896'
WTLD_NS = '12897'
WTLD_ON = '12899'
WTLD_PE = '12900'
WetlandList = [WTLD_AB,WTLD_BC,WTLD_MB,WTLD_NB,WTLD_NS,WTLD_ON,WTLD_PE]

RADN ='12826'
RDN = '12885' # health region
RadonList = [RADN,RDN]

ANSI_ON = '12239'
AnsiList = [ANSI_ON]

# TEST
server_environment = 'prod'
server_config_file = r"\\cabcvan1gis006\GISData\ERISServerConfig.ini"
server_config = server_loc_config(server_config_file,server_environment)
connectionString = r'ERIS_GIS/gis295@GMPRODC.glaciermedia.inc'

report_path = server_config["noninstant"]
reportcheck_path = server_config["reportcheck"]
viewer_path = server_config["viewer"]
upload_link =  server_config["viewer_upload"] + r"/ErisInt/BIPublisherPortal_prod/Viewer.svc/"

# report_path = r"\\cabcvan1obi002\ErisData\Reports\prod\noninstant_reports"
# reportcheck_path = r'\\cabcvan1obi002\ErisData\Reports\prod\reportcheck'
# viewer_path = r"\\CABCVAN1OBI002\ErisData\Reports\prod\viewer"
# upload_link = r"http://CABCVAN1OBI002/ErisInt/BIPublisherPortal_prod/Viewer.svc/"

datalyr_folder = r"\\cabcvan1gis006\GISData\PSR_CAN\python\mxd"
connectionPath = r"\\cabcvan1gis006\GISData\PSR_CAN\python"

# ORDER SETTING
orderGeomlyrfile_point = os.path.join(datalyr_folder,r"SiteMaker.lyr")
orderGeomlyrfile_polyline = os.path.join(datalyr_folder,r"orderLine.lyr")
orderGeomlyrfile_polygon = os.path.join(datalyr_folder,r"orderPoly.lyr")
bufferlyrfile = os.path.join(datalyr_folder,r"buffer.lyr")
gridlyrfile = os.path.join(datalyr_folder,r"Grid_hollow.lyr")

# TOPO GEOLOGY WETLAND WELLS CONTOUR #
# MASTERFILE #
masterlyr_toporama = r"\\cabcvan1gis006\GISData\PSR_CAN\masterfile\toporama_master.shp"
masterlyr_topoBC = r"\\cabcvan1gis006\GISData\PSR_CAN\masterfile\BC_topo_master.shp"
masterlyr_topoON = r"\\cabcvan1gis006\GISData\PSR_CAN\masterfile\ON_topo_master.shp"
masterlyr_topoNS = r"\\cabcvan1gis006\GISData\PSR_CAN\masterfile\NS_topo_master.shp"
masterlyr_dem = r"\\cabcvan1gis006\GISData\PSR_CAN\masterfile\dem_hillshade_master.shp"
masterlyr_shadedrelief = r"\\cabcvan1gis006\GISData\PSR_CAN\masterfile\dem_hillshade_master.shp"#data_shadedrelief = masterlyr_dem
masterlyr_soil = r"\\cabcvan1gis006\GISData\PSR_CAN\masterfile\soil_master.shp"
masterlyr_wetland = r"\\cabcvan1gis006\GISData\PSR_CAN\masterfile\wetland_master.shp"
masterlyr_bedrock = r"\\cabcvan1gis006\GISData\PSR_CAN\masterfile\bedrock_master.shp"
masterlyr_surficial =r"\\cabcvan1gis006\GISData\PSR_CAN\masterfile\surficial_master.shp"

# LAYER #
topowhitelyrfile = os.path.join(datalyr_folder,r"topo_white.lyr")
topolyrfile =os.path.join(datalyr_folder,r"Topo.lyr")
relieflyrfile =os.path.join(datalyr_folder,r"relief.lyr")
eris_wells = os.path.join(datalyr_folder,r"ErisWellSites.lyr")   #which contains water, oil/gas wells etc.
datalyr_wetland = os.path.join(datalyr_folder,r"wetland.lyr")
datalyr_psw = os.path.join(datalyr_folder,r"PSW.lyr")
datalyr_geologyS = os.path.join(datalyr_folder,r"geology_Surficial.lyr")
datalyr_geologyB =os.path.join(datalyr_folder,r"geology_Bedrock.lyr")
datalyr_contour = os.path.join(datalyr_folder,r"contours_largescale.lyr")
kmllyr_wetland = os.path.join(datalyr_folder,r"wetland_kml.lyr")
kmllyr_psw = os.path.join(datalyr_folder,r"psw_kml.lyr")
datalyr_ansi = os.path.join(datalyr_folder,r"ansi.lyr")
datalyr_piplineAB = os.path.join(datalyr_folder,r"Pipeline_AB.lyr")
kmldatalyr_piplineAB = os.path.join(datalyr_folder,r"Pipeline_AB_kml.lyr")
datalyr_pipInsAB = os.path.join(datalyr_folder,r"PipelineInstallation_AB.lyr")
datalyr_pipelineBC = os.path.join(datalyr_folder, r"Pipeline_BC.lyr")
kmldatalyr_pipelineBC = os.path.join(datalyr_folder, r"Pipeline_BC_kml.lyr")
datalyr_pipelineROWBC = os.path.join(datalyr_folder, r"PipelineROW_BC.lyr")
kmldatalyr_pipelineROWBC = os.path.join(datalyr_folder, r"PipelineROW_BC_kml.lyr")
datalyr_pipelineSK = os.path.join(datalyr_folder, r"Pipeline_SK.lyr")
kmldatalyr_pipelineSK = os.path.join(datalyr_folder, r"Pipeline_SK_kml.lyr")

# DIRECTORY #
tifdir_toporama = r"\\cabcvan1fpr009\DATA_GIS_2\TOPO_DATA_CANADA\50k_utm_tif"
tifdir_topoBC = r"\\cabcvan1fpr009\DATA_GIS_2\TOPO_DATA_CANADA\BC_20k_tif"
imgdir_dem = r"\\cabcvan1fpr009\US_DEM\DEM1"
path_shadedrelief = r"\\cabcvan1fpr009\US_DEM\hillshade1"

# DATA #
data_geol_bedrock = r'\\cabcvan1gis006\GISData\Data\PSR_CAN\PSR_CAN.gdb\bedrock_geology'
data_geol_bedrock_Prov_gdb= r"\\cabcvan1gis006\GISData\Data\PSR_CAN\Bedrock_provs.gdb"
data_geol_surficial = r'\\cabcvan1gis006\GISData\Data\PSR_CAN\PSR_CAN.gdb\surficial_geology'
data_wetland = r'\\cabcvan1gis006\GISData\Data\PSR_CAN\PSR_CAN.gdb\wetland_merged'
data_soilGDB = r'\\cabcvan1gis006\GISData\Data\PSR_CAN\PSR_CAN.gdb'
data_soilScape = r'\\cabcvan1gis006\GISData\Data\PSR_CAN\PSR_CAN.gdb\Soil_Landscape'
data_radonHR = r'\\cabcvan1gis006\GISData\Data\PSR_CAN\PSR_CAN.gdb\radon_HR'
data_radonPriv = r'\\cabcvan1gis006\GISData\Data\PSR_CAN\PSR_CAN.gdb\radon_private'

# MXD #
mxdfile_topoCA = r"\\cabcvan1gis006\GISData\PSR_CAN\python\mxd\TOPOLayout.mxd"
mxdfile_topoON = r"\\cabcvan1gis006\GISData\PSR_CAN\python\mxd\ERISOBMLayout.mxd"
mxdfile_topoNS = r"\\cabcvan1gis006\GISData\PSR_CAN\python\mxd\NStopoLayout.mxd"
mxdMMfile_topoCA = r"\\cabcvan1gis006\GISData\PSR_CAN\python\mxd\TOPOLayoutMM.mxd"
mxdMMfile_topoNS = r"\\cabcvan1gis006\GISData\PSR_CAN\python\mxd\NStopoLayoutMM.mxd"
mxdMMfile_topoON = r"\\cabcvan1gis006\GISData\PSR_CAN\python\mxd\ERISOBMLayoutMM.mxd"
mxdfile_relief =  r"\\cabcvan1gis006\GISData\PSR_CAN\python\mxd\shadedrelief.mxd"
mxdMMfile_relief =  r"\\cabcvan1gis006\GISData\PSR_CAN\python\mxd\shadedreliefMM.mxd"
mxdfile_wetland = r"\\cabcvan1gis006\GISData\PSR_CAN\python\mxd\wetland.mxd"
mxdMMfile_wetland = r"\\cabcvan1gis006\GISData\PSR_CAN\python\mxd\wetlandMM.mxd"
mxdfile_ansi = r"\\cabcvan1gis006\GISData\PSR_CAN\python\mxd\ERISANSILayout.mxd"
mxdfile_ansireport = r"\\cabcvan1gis006\GISData\PSR_CAN\python\mxd\ERISANSIReport.mxd"
mxdMMfile_ansi = r"\\cabcvan1gis006\GISData\PSR_CAN\python\mxd\ERISANSILayoutMM.mxd"
#mxdfile_flood = r"\\cabcvan1gis006\GISData\PSR_CAN\python\mxd\flood.mxd"
#mxdMMfile_flood = r"\\cabcvan1gis006\GISData\PSR_CAN\python\mxd\floodMM.mxd"
mxdfile_geolB = r"\\cabcvan1gis006\GISData\PSR_CAN\python\mxd\geology_bedrock.mxd"
mxdfile_geolS = r"\\cabcvan1gis006\GISData\PSR_CAN\python\mxd\geology_surficial.mxd"
mxdMMfile_geolB = r"\\cabcvan1gis006\GISData\PSR_CAN\python\mxd\geology_bedrockMM.mxd"
mxdMMfile_geolS = r"\\cabcvan1gis006\GISData\PSR_CAN\python\mxd\geology_surficialMM.mxd"
mxdfile_geolB_prov = r"\\cabcvan1gis006\GISData\PSR_CAN\python\mxd\geology_bedrock_Prov.mxd"
mxdfile_geolS_prov = r"\\cabcvan1gis006\GISData\PSR_CAN\python\mxd\geology_surficial_Prov.mxd"
mxdMMfile_geolB_prov = r"\\cabcvan1gis006\GISData\PSR_CAN\python\mxd\geology_bedrock_ProvMM.mxd"
mxdMMfile_geolS_prov = r"\\cabcvan1gis006\GISData\PSR_CAN\python\mxd\geology_surficial_ProvMM.mxd"
mxdfile_soil = r"\\cabcvan1gis006\GISData\PSR_CAN\python\mxd\soil.mxd"
mxdMMfile_soil = r"\\cabcvan1gis006\GISData\PSR_CAN\python\mxd\soilMM.mxd"
mxdfile_wells = r"\\cabcvan1gis006\GISData\PSR_CAN\python\mxd\wells.mxd"
mxdMMfile_wells = r"\\cabcvan1gis006\GISData\PSR_CAN\python\mxd\wellsMM.mxd"

# GEology Bedrock Lookup Table #
AB_bedrock = OrderedDict([("FID_1",'Unit ID'),
        ('Unit_Name','Unit Name'),
        ("Lithology", 'Lithology'),
        ("Environ","Environment"),
        ("Age", 'Age'),
        ("GeolRegion",'Geological Regions')])

BC_bedrock = OrderedDict([('strat_unit','Unit ID'),
        ("era", 'Era'),
        ("period","Period"),
        ("strat_age", 'Age of Strata'),
        ("strat_name",'Name of strata'),
        ("gp_suite", 'Group'),
        ("fm_lithodm",'Formation Types'),
        ("rock_class", 'Rock Class'),
        ("rock_type",'Rock Type'),
        ("rock_txtr", 'Rock Texture'),
        ("age_max",'Maximum Age'),
        ("age_min", 'Minimum Age'),
        ("belt",'Physiogeological region'),
        ("terrane", 'Terrane'),
        ("basin",'Basin'),
        ("basin_age", 'Basin Age'),
        ("source_ref","Source Reference")])

ON_bedrock = OrderedDict([('GEOLOGY_','Unit ID'),
        ("UNITNAME_P", 'Unit Name'),
        ("ROCKTYPE_P","Rock Type"),
        ("STRAT_P", 'Strata'),
        ("SUPEREON_P",'Super Eon'),
        ("EON_P", 'Eon'),
        ("ERA_P",'Era'),
        ("PERIOD_P", 'Period'),
        ("EPOCH_P",'Epoch'),
        ("PROVINCE_P", 'Province'),
        ("TECTZONE_P",'Tectonic Zone')])

MB_bedrock = OrderedDict([('UNIT_CODE','Unit ID'),
        ("UNIT", 'Unit Name'),
        ("SUBUNIT",'Sub Unit Name'),
        ("UNIT_DESCR", 'Unit Description'),
        ("EON", 'Eon'),
        ("ERA",'Era'),
        ("PERIOD", 'Period'),
        ("EPOCH",'Epoch'),
        ("PROVINCE", 'Province'),
        ("LITHOTEC",'Lithology')])

NB_bedrock = OrderedDict([('ID','Unit ID'),
        ("Group_", 'Group'),
        ("Age","Age"),
        ("Lithology", 'Lithology'),
        ("Formation",'Formation')])

NL_bedrock = OrderedDict([('UNIT_ID','Unit ID'),
        ("G_UNITNAME", 'Unit Name'),
        ("L_DOMLABEL","Dominant Label"),
        ("L_ROCKTYPE", 'Rock Type'),
        ("G_AGERANGE",'Age Range'),
        ("L_TECTDIV", 'Tectonic Division (lithofacies)'),
        ("L_TECTSUB",'Tectonic subdivision (lithofacies)')])

NS_bedrock = OrderedDict([('LU_CODE','Unit ID'),
        ("RX_MODE","Unit Name"),
        ("RX_FAMILY", 'Rock Family'),
        ("RX_TYPE", 'Rock Type'),
        ("COMM",'Comments')])

QC_bedrock = OrderedDict([('CodeEtq','Unit ID'),
        ("Descript", 'Rock Description'),
        ("Age","Age"),
        ("Periode", 'Periode'),
        ("Strati",'Strata')])

SK_bedrock = OrderedDict([('ROCK_CODE','Unit ID'),
        ("GROUP_", 'Group'),
        ("FORMATION","Formation"),
        ("MEMBER", 'Member'),
        ("ROCK_TYPE",'Rock Type'),
        ("EON", 'Eon'),
        ("ERA",'Era'),
        ("PERIOD", 'Period')])

YT_bedrock = OrderedDict([('UNIT_250K','Unit ID'),
        ("SUPERGROUP", 'Supergroup'),
        ("GP_SUITE","Group"),
        ("FORMATION","Formation"),
        ("NAME_1",'Name'),
        ("TERRANE", 'Terrane'),
        ("TECT_ELEM",'Tectonic Element'),
        ("ROCK_CLASS", 'Rock Type'),
        ("ROCK_SUBCL","Sub Rock Type"),
        ("SHORT_DESC", 'Description'),
        ("ROCK_MAJOR", 'Major lithology'),
        ("ROCK_NOTES", 'Rock Notes'),
        ("ERA_MAX", 'Maximum Era'),
        ("PERIOD_MAX", 'Maximum Period'),
        ("EPOCH_MIN", 'Maximum Epoch'),
        ("STAGE_MIN", 'Maximum Age'),
        ("ERA_MIN", 'Minimum Era'),
        ("PERIOD_MIN", 'Minimum Period'),
        ("EPOCH_MIN", 'Minimum Epoch'),
        ("STAGE_MIN", 'Minimum Age')])

# surficial Look up table
AB_surficial = OrderedDict([('MAP_LABEL', "Unit ID"),
        ('GENETIC_GR',"Unit Name"),
        ('UNIT_DESC', "Unit Description"),
        ('TEXTURE',"Texture"),
        ('AGE',"Age")])

ON_surficial = OrderedDict([('DEPOSIT_TYPE_COD', "Unit ID"),
        ('GEOLOGIC_DEPOSIT',"Geological Deposit"),
        ('DEPOSIT_AGE', "Deposit Age"),
        ('PRIM_MAT',"Primary Material"),
        ('SEC_MAT',"Secondary Material"),
        ('PRIM_GEN',"Primary General"),
        ('PRIM_GEN_MOD',"Primary General Modifier"),
        ('VENEER',"Veneer"),
        ('EPISODE',"Episode"),
        ('SUBEPISODE',"Sub Episode"),
        ('STRAT_MOD',"Strata Modifier"),
        ('PROVENANCE',"Provenance"),
        ('CARB_CONTENT',"Carbon Content"),
        ('FORMATION',"Formation"),
        ('PERMEABILITY',"Permeability"),
        ('MATERIAL_DESCRIP',"Material Description")])

MB_surficial = OrderedDict([('DERIVED_LE', "Unit ID"),
        ('LEGEND_DES',"Legend Description")])

NL_surficial = OrderedDict([('COLORCODE', "Unit ID"),
        ('LEGEND',"Legend"),
        ('DESCRIPTN',"Legend Description")])

NS_surficial = OrderedDict([('LEGEND_ID', "Unit ID"),
        ('PERIOD',"Period"),
        ('GLACIATION',"Glaciation"),
        ('LEGEND_LAB', "Legend Label"),
        ('UNIT_DESC',"Description"),
        ('TOPOGRAPHY',"Topography"),
        ('THICKNESS', "Thickness"),
        ('ORIGIN',"Origin"),
        ('EC_EN_SIG',"Significance")])

SK_surficial = OrderedDict([('CODE', "Unit ID"),
        ('UNIT',"Unit Name")])

#CA_surficial =OrderedDict([('ULABEL', "Unit"),
#        ('UTYPE',"Unit TYPE"),
#        ("HYDRO")])
#
#CA_bedrock = OrderedDict([('UNIT', "Unit"),
#        ('RXTP',"Rock Type"),
#        ("SUBRXTP","Sub Rock Type"),
#        ("ERA","PERIOD","EPOCH"),
#        ("GEOLPROV")])