# -*- coding: utf-8 -*-

"""
Format SHPO Data

Michael Troyer

michael.troyer@usda.gov

"""


import datetime
import os
import re
import traceback

import arcpy

arcpy.env.addOutputsToMap = False
arcpy.env.overwriteOutput = True

SITE_REGEX = r'^[0-9]{1,2}[A-Z]{2}\.[0-9]{1,5}\.*[0-9]*$'
SURVEY_REGEX = r'^[A-Z]{2}\.[A-Z]{2}\.[A-Z]{1,2}[0-9]{1,5}$'

UTM_ZONES = r'\Templates\utm_zones_co.shp'


def get_zone(features):
    zone_lyr = arcpy.MakeFeatureLayer_management(UTM_ZONES, 'in_memory\\zones')
    arcpy.SelectLayerByLocation_management(zone_lyr, "INTERSECT", features)
    with arcpy.da.SearchCursor(zone_lyr, 'UTM_Zone') as cur:
        zone = max([row[0] for row in cur])
    arcpy.Delete_management(zone_lyr)
    return zone


def blast_my_cache():
    """Delete in memory tables and feature classes
       reset to original worksapce when done"""
    # get the original workspace location
    orig_workspace = arcpy.env.workspace
    # Set the workspace to in_memory
    arcpy.env.workspace = "in_memory"
    # Delete all in memory feature classes
    fcs = arcpy.ListFeatureClasses()
    if len(fcs) > 0:
        for fc in fcs:
            arcpy.Delete_management(fc)
    # Delete all in memory tables
    tbls = arcpy.ListTables()
    if len(tbls) > 0:
        for tbl in tbls:
            arcpy.Delete_management(tbl)
    # Reset the workspace
    arcpy.env.workspace = orig_workspace


def buildWhereClauseFromList(table, field, valueList):
    """Takes a list of values and constructs a SQL WHERE
    clause to select those values within a given field and table."""
    # Add DBMS-specific field delimiters
    fieldDelimited = arcpy.AddFieldDelimiters(arcpy.Describe(table).path, field)
    # Determine field type
    fieldType = arcpy.ListFields(table, field)[0].type
    # Add single-quotes for string field values
    if str(fieldType) == 'String':
        valueList = ["'%s'" % value for value in valueList]
    # Format WHERE clause in the form of an IN statement
    whereClause = "%s IN(%s)" % (fieldDelimited, ', '.join(map(str, valueList)))
    return whereClause


class Toolbox(object):   
    def __init__(self):
        self.label = "Format_SHPO_Data"
        self.alias = "Format_SHPO_Data"
        
        # List of tool classes associated with this toolbox
        self.tools = [Format_SHPO_Survey_Data, Format_SHPO_Site_Data]


class Format_SHPO_Survey_Data(object):
    def __init__(self):
        self.label = "Format_Survey_Data"
        self.description = ""
        self.canRunInBackground = True
        
    def getParameterInfo(self):
        
        input_FC=arcpy.Parameter(
            displayName="Input Feature Class",
            name="Input_FC",
            datatype="Feature Class",
            )
        subselect=arcpy.Parameter(
            displayName="Selection based on case value",
            name="Select_Boolean",
            datatype="Boolean",
            parameterType="Optional",
            enabled = "False",
            )
        case_field=arcpy.Parameter(
            displayName="Select Feature Case Field",
            name="Select_Field",
            datatype="String",
            parameterType="Optional",
            enabled = "False",
            )
        case_value=arcpy.Parameter(
            displayName="Select Feature Case Value",
            name="Select_Value",
            datatype="String",
            parameterType="Optional",
            enabled = "False",
            )
        # SHPO Fields
        # DOC_
        shpo_id=arcpy.Parameter(
            displayName="SHPO Survey ID",
            name="SHPO_ID",
            datatype="String",
            parameterType="Optional",
            )
        # AGENCY_
        agency_id=arcpy.Parameter(
            displayName="Agency Survey ID",
            name="Agency_ID",
            datatype="String",
            parameterType="Optional",
            )
        # TITLE
        title=arcpy.Parameter(
            displayName="Survey Title",
            name="Title",
            datatype="String",
            parameterType="Optional",
            )
        # AUTHOR
        author=arcpy.Parameter(
            displayName="Author",
            name="Author",
            datatype="String",
            parameterType="Optional",
            )
        # SURVEY_TYPE
        survey_type=arcpy.Parameter(
            displayName="Survey Type",
            name="Survey_Type",
            datatype="String",
            parameterType="Optional",
            )
        # SITE_COUNT
        site_count=arcpy.Parameter(
            displayName="Site Count",
            name="Site_Count",
            datatype="Long",
            parameterType="Optional",
            )        
        # IF_COUNT
        if_count=arcpy.Parameter(
            displayName="IF Count",
            name="IF_Count",
            datatype="Long",
            parameterType="Optional",
            )
        # ELIGIBLE_COUNT
        eligible_count=arcpy.Parameter(
            displayName="Eligible Count",
            name="Eligible_Count",
            datatype="Long",
            parameterType="Optional",
            )
        # COMMENTS
        comments=arcpy.Parameter(
            displayName="Comments",
            name="Comments",
            datatype="String",
            parameterType="Optional",
            )
        # CONFIDENCE
        confidence=arcpy.Parameter(
            displayName="Confidence",
            name="Confidence",
            datatype="String",
            parameterType="Optional",
            )
        # Output
        output_FC=arcpy.Parameter(
            displayName="Output Feature Class",
            name="Output_FC",
            datatype="Feature Class",
            direction="Output",
            )
        return [
            input_FC, subselect, case_field, case_value,
            shpo_id, agency_id, title, author, survey_type,
            site_count, if_count, eligible_count,
            comments, confidence,
            output_FC, 
            ]


    def isLicensed(self):
        return True


    def updateParameters(self, params):
        # input_FC, subselect, case_field, case_value = params[:4]
        (input_FC, subselect, case_field, case_value,
        shpo_id, agency_id, title, author, survey_type,
        site_count, if_count, eligible_count,
        comments, confidence,
        output_FC) = params

        input_FC.filter.list = ["Polygon"]
        
        if input_FC.value:
            subselect.enabled = "True"
        else:
            subselect.enabled = "False"
            
        if subselect.value == 1:
            desc = arcpy.Describe(input_FC.value)
            fields = desc.fields
            field_list = [f.name for f in fields if f.type in ["String", "Integer", "SmallInteger"]]      
            case_field.enabled = "True"
            case_field.filter.type = "ValueList"
            case_field.filter.list = field_list
        else:
            case_field.value = ""
            case_field.enabled = "False"
           
        if case_field.value:
            vals = set([r[0] for r in arcpy.da.SearchCursor(input_FC.value, case_field.value)])
            vals = sorted([v for v in vals if v])
            case_value.enabled = "True"
            case_value.filter.type = "ValueList"
            case_value.filter.list = vals
        else:
            case_value.value = ""
            case_value.enabled = "False"
        
        survey_type.filter.type = "ValueList"
        survey_type.filter.list = ['Class I', 'Class II', 'Class III']

        confidence.filter.type = "ValueList"
        confidence.filter.list = ['High', 'Medium', 'Low']

        return


    def updateMessages(self, params):
        shpo_id = params[4]

        if shpo_id.value:
            if not re.match(SURVEY_REGEX, shpo_id.value):
                shpo_id.setErrorMessage("SHPO survey ID must be in format xx.SC.xxXXX (e.g. LR.SC.NR999)")
                        
        return


    def execute(self, params, messages):
        blast_my_cache()

        (input_FC, subselect, case_field, case_value,
        shpo_id, agency_id, title, author, survey_type,
        site_count, if_count, eligible_count,
        comments, confidence,
        output_FC) = params

        # for param in params:
        #     arcpy.AddMessage('{} [Value: {}], [Text: {}]'.format(
        #         param.name, param.value, param.valueAsText
        #         ))

        template = r'\Templates\survey_ply_tmp.shp'
        
        try:
            # Check for a subselection
            if subselect.value:
                where = buildWhereClauseFromList(
                    input_FC.value, case_field.value, [case_value.value]
                    )
                arcpy.AddMessage(where)
                tmp = arcpy.MakeFeatureLayer_management(input_FC.value, "in_memory\\tmp", where)
            else:
                tmp = arcpy.MakeFeatureLayer_management(input_FC.value, "in_memory\\tmp")
                        
            # Enforce single-part
            if int(arcpy.GetCount_management(tmp).getOutput(0)) > 1:
                lyr = arcpy.Dissolve_management(tmp, 'in_memory\\lyr')    
            else:
                lyr = tmp

            # Get the zone
            zone = get_zone(lyr)
            
            # Clear template and append feature
            template_lyr = arcpy.MakeFeatureLayer_management(template, 'in_memory\\template')
            arcpy.DeleteRows_management(template_lyr)
            arcpy.Append_management(lyr, template_lyr, "NO_TEST")
            
            #Calculate fields
            update_fields = (
                'DOC_',
                'AGENCY_',
                'TITLE',
                'AUTHOR',
                'DATE',
                'SURV_TYPE',
                'SITE_COUNT',
                'IF_COUNT',
                'EL_COUNT',
                'ZONE',
                'COMMENTS',
                'SOURCE',
                'CONF',
            )

            # Attribute calculations
            with arcpy.da.UpdateCursor(template_lyr, update_fields) as cur:
                for row in cur:
                    row[0] = shpo_id.value if shpo_id.value else ''
                    row[1] = agency_id.value if agency_id.value else ''
                    row[2] = title.value if title.value else ''
                    row[3] = author.value if author.value else ''
                    row[4] = datetime.date.today()
                    row[5] = survey_type.value if survey_type.value else ''
                    row[6] = site_count.value if site_count.value else 0
                    row[7] = if_count.value if if_count.value else 0
                    row[8] = eligible_count.value if eligible_count.value else 0
                    row[9] = zone
                    row[10] = comments.value if comments.value else ''
                    row[11] = 'NRCS Colorado'
                    row[12] = {
                        'High': 'HC',
                        'Medium': 'MC',
                        'Low': 'LC',
                        }[confidence.value] if confidence.value else ''

                    cur.updateRow(row)

            # Spatial calculations
            for field, value in [
                ('ACRES', "!shape.area@ACRES!"),
                ('X', "!shape.centroid.X!"),
                ('Y', "!shape.centroid.Y!"),
                ('AREA', "!shape.area@SQUAREMETERS!"),
                ('PERIMETER', "!shape.length@METERS!"),
                ]:
                arcpy.CalculateField_management(template_lyr, field, value, "PYTHON_9.3")

            #output shape and save
            arcpy.CopyFeatures_management(template_lyr, output_FC.value)
            
        except:
            arcpy.AddError(traceback.format_exc())    

        finally:
            try:
                for item in [tmp, lyr, template_lyr]:
                    arcpy.Delete_management(item)
            except:
                pass
            blast_my_cache()

            #Clear template for future use
            arcpy.DeleteRows_management(template)
            
        return                      



###############################################################################



class Format_SHPO_Site_Data(object):
    def __init__(self):
        self.label = "Format_Site_Data"
        self.description = ""
        self.canRunInBackground = True
        
    def getParameterInfo(self):
        
        input_FC=arcpy.Parameter(
            displayName="Input Feature Class",
            name="Input_FC",
            datatype="Feature Class",
            )
        subselect=arcpy.Parameter(
            displayName="Selection based on case value",
            name="Select_Boolean",
            datatype="Boolean",
            parameterType="Optional",
            enabled = "False",
            )
        case_field=arcpy.Parameter(
            displayName="Select Feature Case Field",
            name="Select_Field",
            datatype="String",
            parameterType="Optional",
            enabled = "False",
            )
        case_value=arcpy.Parameter(
            displayName="Select Feature Case Value",
            name="Select_Value",
            datatype="String",
            parameterType="Optional",
            enabled = "False",
            )
                   
        # SHPO Fields
        # SITE_
        site_id=arcpy.Parameter(
            displayName="Site ID",
            name="SITE_ID",
            datatype="String",
            parameterType="Optional",
            )
        # SITE_NAME
        site_name=arcpy.Parameter(
            displayName="Site Name",
            name="Site_Name",
            datatype="String",
            parameterType="Optional",
            )
        # AGENCY_
        agency_id=arcpy.Parameter(
            displayName="Agency Report ID",
            name="Agency_ID",
            datatype="String",
            parameterType="Optional",
            )
        # SHPO ID
        shpo_id=arcpy.Parameter(
            displayName="SHPO Report ID",
            name="SHPO_ID",
            datatype="String",
            parameterType="Optional",
            )
        # SITE_TYPE
        site_type=arcpy.Parameter(
            displayName="Site Type",
            name="Site_Type",
            datatype="String",
            parameterType="Optional",
            )
        # SITE_DESCRIPTION
        site_desc=arcpy.Parameter(
            displayName="Site Description",
            name="Site_Desc",
            datatype="String",
            parameterType="Optional",
            )
        # LINEAR
        linear=arcpy.Parameter(
            displayName="Linear",
            name="Linear",
            datatype="String",
            parameterType="Optional",
            )
        # ELIGIBILITY
        eligibility=arcpy.Parameter(
            displayName="Eligibility",
            name="Eligibility",
            datatype="String",
            parameterType="Optional",
            )
        # COMMENTS
        comments=arcpy.Parameter(
            displayName="Comments",
            name="Comments",
            datatype="String",
            parameterType="Optional",
            )
        # CONFIDENCE
        confidence=arcpy.Parameter(
            displayName="Confidence",
            name="Confidence",
            datatype="String",
            parameterType="Optional",
            )
        # Output
        output_FC=arcpy.Parameter(
            displayName="Output Feature Class",
            name="Output_FC",
            datatype="Feature Class",
            direction="Output",
            )

        return [
            input_FC, subselect, case_field, case_value,
            site_id, site_name, agency_id, shpo_id, site_type, site_desc,
            linear, eligibility, comments, confidence,
            output_FC,
            ]


    def isLicensed(self):
        return True


    def updateParameters(self, params):
        (input_FC, subselect, case_field, case_value,
        site_id, site_name, agency_id, shpo_id, site_type, site_desc,
        linear, eligibility, comments, confidence,
        output_FC) = params

        input_FC.filter.list = ["Polygon"]
        
        if input_FC.value:
            subselect.enabled = "True"
        else:
            subselect.enabled = "False"
            
        if subselect.value == 1:
            desc = arcpy.Describe(input_FC.value)
            fields = desc.fields
            field_list = [f.name for f in fields if f.type in ["String", "Integer", "SmallInteger"]]      
            case_field.enabled = "True"
            case_field.filter.type = "ValueList"
            case_field.filter.list = field_list
        else:
            case_field.value = ""
            case_field.enabled = "False"
           
        if case_field.value:
            vals = set([r[0] for r in arcpy.da.SearchCursor(input_FC.value, case_field.value)])
            vals = sorted([v for v in vals if v])
            case_value.enabled = "True"
            case_value.filter.type = "ValueList"
            case_value.filter.list = vals
        else:
            case_value.value = ""
            case_value.enabled = "False"
        
        site_type.filter.type = "ValueList"
        site_type.filter.list = ['Historic', 'Prehistoric', 'Multicomponent']

        site_type.filter.type = "ValueList"
        site_type.filter.list = ['Historic', 'Prehistoric', 'Multicomponent']

        linear.filter.type = "ValueList"
        linear.filter.list = ['Linear', 'Non-Linear']

        eligibility.filter.type = "ValueList"
        eligibility.filter.list = [
            'Eligible', 'Not Eligible', 'Needs Data',
            'Supporting', 'Non-Supporting',
            'Contributing', 'Non-Contributing',
            ]

        confidence.filter.type = "ValueList"
        confidence.filter.list = ['High', 'Medium', 'Low']

        return


    def updateMessages(self, params):
        (input_FC, subselect, case_field, case_value,
        site_id, site_name, agency_id, shpo_id, site_type, site_desc,
        linear, eligibility, comments, confidence,
        output_FC) = params

        if site_id.value:
            if not re.match(SITE_REGEX, site_id.value):
                site_id.setErrorMessage(
                    "SHPO site ID must be in format 5XX.xxxx (e.g. 5LR.1234 or 5LR.1234.1)"
                    )
        if shpo_id.value:
            if not re.match(SURVEY_REGEX, shpo_id.value):
                shpo_id.setErrorMessage("SHPO survey ID must be in format xx.SC.xxXXX (e.g. LR.SC.NR999)")
                
        return


    def execute(self, params, messages):
        blast_my_cache()

        (input_FC, subselect, case_field, case_value,
        site_id, site_name, agency_id, shpo_id, site_type, site_desc,
        linear, eligibility, comments, confidence,
        output_FC) = params

        # for param in params:
        #     arcpy.AddMessage('{} [Value: {}], [Text: {}]'.format(
        #         param.name, param.value, param.valueAsText
        #         ))

        template = r'\Templates\site_ply_tmp.shp'

        try:
            # Check for a subselection
            if subselect.value:
                where = buildWhereClauseFromList(
                    input_FC.value, case_field.value, [case_value.value]
                    )
                tmp = arcpy.MakeFeatureLayer_management(input_FC.value, "in_memory\\tmp", where)
            else:
                tmp = arcpy.MakeFeatureLayer_management(input_FC.value, "in_memory\\tmp")
                        
            # Enforce single-part
            if int(arcpy.GetCount_management(tmp).getOutput(0)) > 1:
                lyr = arcpy.Dissolve_management(tmp, 'in_memory\\lyr')    
            else:
                lyr = tmp

            # Get the zone
            zone = get_zone(lyr)
       
            # Clear template and append feature
            template_lyr = arcpy.MakeFeatureLayer_management(template, 'in_memory\\template')
            arcpy.DeleteRows_management(template_lyr)
            arcpy.Append_management(lyr, template_lyr, "NO_TEST")
            
            #Calculate fields
            update_fields = (
                'SITE_',
                'SITE_NAME',
                'AGENCY_',
                'SHPO_ID',
                'DATE',
                'SITE_TYPE',
                'SITE_DESC',
                'LINEAR',
                'ELIGIBILIT',
                'ZONE',
                'COMMENTS',
                'SOURCE',
                'BND_CMPLT',
                'CONF',
                )

            # Attribute calculations
            with arcpy.da.UpdateCursor(template_lyr, update_fields) as cur:
                for row in cur:
                    row[0] = site_id.value if site_id.value else ''
                    row[1] = site_name.value if site_name.value else ''
                    row[2] = agency_id.value if agency_id.value else ''
                    row[3] = shpo_id.value if shpo_id.value else ''
                    row[4] = datetime.date.today()
                    row[5] = site_type.value if site_type.value else ''
                    row[6] = site_desc.value if site_desc.value else ''
                    row[7] = 1 if linear.value == 'Linear' else 0
                    row[8] = eligibility.value if eligibility.value else ''
                    row[9] = zone
                    row[10] = comments.value if comments.value else ''
                    row[11] = 'NRCS Colorado'
                    row[12] = 'Y'
                    row[13] = {
                        'High': 'HC',
                        'Medium': 'MC',
                        'Low': 'LC',
                        }[confidence.value] if confidence.value else ''

                    cur.updateRow(row)

            # Spatial calculations
            for field, value in [
                ('ACRES', "!shape.area@ACRES!"),
                ('X', "!shape.centroid.X!"),
                ('Y', "!shape.centroid.Y!"),
                ('AREA', "!shape.area@SQUAREMETERS!"),
                ('PERIMETER', "!shape.length@METERS!"),
                ]:
                arcpy.CalculateField_management(template_lyr, field, value, "PYTHON_9.3")

            #output shape and save
            arcpy.CopyFeatures_management(template_lyr, output_FC.value)
            
        except:
            arcpy.AddError(traceback.format_exc())    

        finally:
            try:
                for item in [tmp, lyr, template_lyr]:
                    arcpy.Delete_management(item)
            except:
                pass
            blast_my_cache()

            #Clear template for future use
            arcpy.DeleteRows_management(template)
            
        return   