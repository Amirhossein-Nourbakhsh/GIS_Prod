###Libraries###
import sys, os, string, arcpy, logging
from arcpy import env, mapping


###Constants and Global Variables###
arcpy.env.OverWriteOutput = True

fclass = arcpy.GetParameter(0)

###arcpy- update cursor###
try: 
  cur = arcpy.UpdateCursor(fclass)
  for row in cur:
    if row.Ele_diff < 0.000:
       row.eleRank = -1
    elif row.Ele_diff== 0:
       row.eleRank= 0
    elif row.Ele_diff>0.0000:
       row.eleRank= 1
    else:
       row.eleRank= 100
    cur.updateRow(row)
# release the layer from locks
  del row, cur
  arcpy.SetParameter(1,fclass)
except:
  # If an error occurred, print the message to the screen
  arcpy.AddMessage(arcpy.GetMessages())
