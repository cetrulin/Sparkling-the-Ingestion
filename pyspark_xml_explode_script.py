
#########################################################################################################
############################# PySpark - imports #########################################################
#########################################################################################################

from pyspark import SparkConf, SparkContext
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.storagelevel import StorageLevel
from pyspark.sql import HiveContext


#########################################################################################################
############################# Python - imports #########################################################
#########################################################################################################
from datetime import datetime
from dateutil.relativedelta import relativedelta
import re
#########################################################################################################
# Initialise
spark = SparkSession.builder.getOrCreate()
sc = SparkContext.getOrCreate()
sc.setLogLevel('ERROR')

sqlContext = SQLContext(sc)

#########################################################################################################
# Print all spark configuration
# sc._conf.getAll()

#########################################################################################################

hive_udf_mapping_file   = "/bin/hive_udfs_mapping.py"

#########################################################################################################

#try:    
#    print("Loading Hive UDF map file")
#    exec(open(hive_udf_mapping_file).read())
#except:
    # already loaded the UDFs
#    pass

#########################################################################################################


#########################################################################################################
############################# Python - imports #########################################################
#########################################################################################################
import math
import re
import sys
import random
from functools import reduce
from datetime import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd
#########################################################################################################


# Dont_explode_these_fields input (list of strings): e.g. [‘parent.child.child_field’] 
def flat_and_explode_XML(result,dont_explode_these_fields=[],debug=False): 
   
    # Aux variables to decide if we don't want to explode a parameter called in an specific way
    # TODO: both in one param and feed it as argument
    dont_expand_list=[] #e.g. [‘child_field’] # this operates over a subtree so it doesn't need the path (the last pos in an split by . should work over the next param string)
    
    # Declaring dictionary to save names of fields that will be exploded
    result_names_dict= {}
    
    # Get list of paths to be added to the DF
    paths= parse_xml(result,root_path='root')
    # Now we have the lists of paths to send to function 1: explodePaths. 
    
    
    # Iterate through Dataframe indexed paths and explode if necessary
    for path in paths:
        result, result_cols, result_exploded=explodePath(result,path[0],dont_explode_these_fields,debug,tmpColSeparator='_')
        result_names_dict[result_cols] = str(path[0])
    
    # Separate the fields that have been exploded (were arrays) and the ones that doesn't in two groups for the DF
    exploded_fields = [s for s in result.schema.names  if "EXPLODED" in s]
    clean_paths=[i[0] for i in paths]

    # Select the two group of fields with a parsed alias already (to rename then to appripiate paths and dont get confused with names)
    paths_with_alias=[path+' AS '+str.replace(path,'.','_') for path in clean_paths]
    
    if debug:
        print("All fields except exploded:")
        print(paths_with_alias)
        print("__")
        print("List of fields being exploded:")
        print(flatten_list_of_lists([create_alias_4exploded(exploded,result_names_dict,result) for exploded in exploded_fields]))
    
    result=result.selectExpr(paths_with_alias+flatten_list_of_lists([create_alias_4exploded(exploded,result_names_dict,result) for exploded in exploded_fields]))

    #if debug:
    #    print("RESULTING DATAFRAME")
    #    print(" ")
    #    print(result.schema.names)
    #    print(" ")
    #    print("END OF DATAFRAME")
           
    # Return Dataframe as an union of both lists and dedup duplicated fields 
    #(e.g. arrays exploded that only had one value so they were a leaf node and therefore they are in both lists)
    return result.selectExpr(sorted(result.schema.names))

''' Receives the XML to be parsed through a tree structure'''
def parse_xml(tree,root_path):
   a,b,res = node_paths(tree,root_path)
   #Return res, as we only want the leaf nodes.
   return res     

''' 
Helper function receives a tree and 
returns all paths that have this node as root and all other paths 
'''
def node_paths(tree_node,node_path):
  #if tree is the empty tree:
  if len(tree_node.schema)==0:
    return ([], [])
  else: #tree is a node
    root = node_path # tree_node.schema
    rooted_paths = [[root]]
    unrooted_paths = []
    res_paths = []

    # Iterating Tree_Node's Childs
    for child_name in tree_node.schema.names:
        subtree_path=node_path+'.'+child_name 
        isExploded = False
        colName=str.replace(subtree_path,'root.','') # =childName
        #print ('Iterating: '+child_name+' ____________ path: '+colName)
        
        # DONT EXPLODE HERE AS WE EXPLODE AFTER
        # IF NODE HAVE CHILDS, WE NEED TO SEND THE NODE EXPANDED
        #if isinstance(tree_node.schema[child_name].dataType,ArrayType) and child_name not in dont_expand_list:
        #   subtree=tree_node.selectExpr("explode("+child_name+") AS "+child_name)
            
            #print ('Array -> Exploding it')
            #print ('Numb fields was:'+str(len(subtree.schema.fields))+' / Numb hidden structured fields was:'+str(get_number_of_struct_fields(subtree.schema)))
            
            # Depending of how many fields does the array has we expand the exploded DF or not
        #    if get_number_of_struct_fields(subtree.schema)>1: #or isinstance(subtree.schema.dataType,ArrayType) 
                #print ('Exploded ARRAY - Have childs.')
        #        subtree=subtree.selectExpr(child_name+".*") #There must be a way that consumes less resources
        #        (useable, unuseable, iterable) = node_paths(subtree,subtree_path)               
        #    else:
                #print ('Exploded ARRAY - Dont have expandable childs')
        #        subtree=subtree.select(col(child_name)) 
        #        (useable, unuseable, iterable) = leaf_path(subtree,subtree_path) 
                
        #el
        if isinstance(tree_node.schema[child_name].dataType,StructType):  
            #In this case we need to expand the field before doing anything else
            #print ('Numb fields was:'+str(len(tree_node.schema[child_name].fields))+' / Numb hidden structured fields was:'+str(get_number_of_struct_fields(tree_node.schema[child_name])))
            subtree=tree_node.selectExpr(child_name+".*") #There must be a way that consumes less resources    
            (useable, unuseable, iterable) = node_paths(subtree,subtree_path)
            
        # IF NODE IS LEAF/ NO NEED TO EXPAND (CALL leaf_path)
        else:
            #print ('Dont have or want expandable childs')        
            #Done by calling leaf_path
            subtree=tree_node.select(col(child_name))
            (useable, unuseable, iterable) = leaf_path(subtree,subtree_path)

        # LISTS TRACKING THE VISITED NODES
        if len(useable)>0:             
            for path in useable: 
                unrooted_paths.append(path)
                expression=''
                #rooted_paths.append([root]+path) # this give us a lot of dups in subpaths (it show all the pivotable nodes as many times as branches they do have)
        if len(unuseable)>0:
            for path in unuseable: 
                unrooted_paths.append(path)
        # A THIRD LIST CONTAINS THE FIELDS TO BE ADDED TO THE DATAFRAME (LEAF NODES). THE RESULT OF THIS PROCESS
        if len(iterable)>0:
            for path in iterable: 
                res_paths.append(path)
    return (rooted_paths, unrooted_paths, res_paths)

'''  Helper function receives a leaf and returns all paths that have this node as root if tree is the empty tree:'''
def leaf_path(tree_node,node_path): 
  if len(tree_node.schema)==0:
    return ([], [])
  else: #tree is a node
    root = node_path # tree_node.schema
    rooted_paths = [[root]]
    unrooted_paths = []
    
    # All the subtrees at the level al leafs with a single field. If they can be expanded in more fields then they won't be at this level.
    res_paths=[[str.replace(node_path,'root.','')]]
    return (rooted_paths, unrooted_paths, res_paths)      

''' Check number of fields in an struct that hasnt been exploded yet '''
def get_number_of_struct_fields(schema):
    n_fields=len(re.findall(pattern='StructField\(',string=str(schema.simpleString)))
    return n_fields

''' Component that decides if we should explode or not '''
def explodePath(dataFrame, pathString, avoid_explode, debug=False, tmpColSeparator='___'):
    #if debug: print('START FUNCTION') # DEBUG
    path = pathString.split('.')
    isExploded = False
    colExploded =  tmpColSeparator.join(['EXPLODED'] + path)
    #if debug: print('colExploded '+colExploded) # DEBUG
    colNonArray = []
    schemaList = dataFrame.schema
    #if debug: print('START LOOP') # DEBUG
    for i,col in enumerate(path):
        if debug: print(' col='+''.join(col)+' ¦¦ i='+str(i)+' ¦¦ path='+''.join(path)) # DEBUG
        if isinstance(schemaList[col].dataType,ArrayType) and '.'.join(path) not in avoid_explode: # Not exploding child_fields specified
            if debug: print('isinstance') # DEBUG
            if debug: print ('isExploded='+str(isExploded)) # DEBUG
            if debug: print ('.'.join(path)+"   "+''.join(avoid_explode))
            #if debug: print (dataFrame) # DEBUG
            # ##################################
            # 1 Create temp table to run the query
            sqlContext.registerDataFrameAsTable(dataFrame, str.replace(pathString,'.','_'))
            # 2 Exploding with Lateral View Outer (not supported by Spark DFs) to not delete any records 
            # Here we can deal with the naming of all the exploded variables instead of running a method at the end (TO-DO in the future)
            dataFrame = sqlContext.sql("SELECT * "+" FROM "+str.replace(pathString,'.','_')+" LATERAL VIEW OUTER explode("+str('.'.join([colExploded] + colNonArray + [col]))+") tmpTable AS "+str(colExploded)) \
                        if isExploded == True \
                        else sqlContext.sql("SELECT * "+" FROM "+str.replace(pathString,'.','_')+" LATERAL VIEW OUTER explode("+'.'.join(path[0:i+1])+") tmpTable AS "+colExploded)
            # 3 De-register temp table
            sqlContext.dropTempTable(str.replace(pathString,'.','_')) 
            # ##################################
            #if debug: print (dataFrame) # DEBUG
            isExploded = True
            colNonArray = []
            schemaList = dataFrame.schema[colExploded].dataType
            #if debug: print(schemaList)
        else:
            if debug: print('is not instance') # DEBUG
            colNonArray.append(col)
            schemaList = schemaList[col].dataType
    colName = '.'.join([colExploded] + colNonArray) if isExploded == True else pathString
    return dataFrame, colName, isExploded    
 
''' Create aliases for exploded fields to not loose the fields path when expanding the fields '''
def create_alias_4exploded(exploded, result_names_dict, result):
   if get_number_of_struct_fields(result.selectExpr(exploded).schema)>1: 
       # edit: exploded added also here (at the end) to avoid dups when we dont explode by that value 
       return [exploded+'.'+name+' AS '+str.replace(exploded,'EXPLODED_','')+'_'+name for name in result.selectExpr(exploded+'.*').schema.names]
   else: 
       # All the single structs inside arrays have are given the names below (exploded array only have one struct with one single child)
       # added exploded at the end to avoid some duplicates in field names in edge cases
       return [exploded+' AS '+str.replace(result_names_dict[exploded],'.','_')+'_exploded'] 
 
''' Converts a list of lists in a single list '''
def flatten_list_of_lists(list_of_lists):
    return [item for sublist in list_of_lists for item in sublist] 
    

''' Returns a sublist of values that exclude our selection'''
def exclude_fields(fields, fields_to_exclude):
    final_fields=[]
    for field in fields:
        if field not in fields_to_exclude:
            final_fields.append(field)

    return final_fields



# ########################################################################################################
final_table = ""
final_table_location  =  ""  
#final_file_format = "com.databricks.spark.avro"
final_file_format = "parquet"
source_xml_root_tag= ""     
# ########################################################################################################

# Drop existing Hive table
print("Dropping XML table")
spark.sql("DROP TABLE IF EXISTS " + final_table + " PURGE")
                  
# ########################################################################################################

# columns to avoid adding to the table as they take a lot of resources
# this is the list of parsed columns after exploded, so arrays (as child_fields specified) can be excluded if they have been exploded previously
columns_to_exclude=[]


# ########################################################################################################

job_start_time = datetime.now()
print("ApplicationID:", sc.applicationId)
print("Track URL: http://URL:PORT/history/%s/jobs/" % sc.applicationId)
print("Job start time: ", job_start_time.strftime('%Y/%m/%d %H:%M:%S'))

input_disk_number="partition"
source_location =  ""

xml_input = spark.read.format("com.databricks.spark.xml")\
            .option("rowTag", source_xml_root_tag)\
            .load(source_location) 

# xml_input_eikon.dtypes
xml_input = xml_input.withColumn("source_filename", input_file_name())
xml_input = xml_input.withColumn("disk", lit(input_disk_number))

# Get flatten DF (Structs expanded and arrays exploded) AND EXPLODING
xml_input_tmp=flat_and_explode_XML(xml_input,dont_explode_these_fields=[],debug=True)

# Exclusion of columns and dedup
xml_input_final=xml_input_tmp.select(exclude_fields(xml_input_tmp.schema.names,columns_to_exclude)+[col('')]) #.distinct()  (not deduping)

# Write new table to Hive table
print("Writing XML - to Hive table")
xml_input_final.write \
    .format(final_file_format) \
    .partitionBy("disk")\
    .mode("overwrite") \
    .option("path", final_table_location) \
    .saveAsTable(final_table)
     
# Compute and display time taken once job has completed
job_end_time = datetime.now()
diff = relativedelta(job_end_time, job_start_time)
print("End time: ", job_end_time.strftime('%Y/%m/%d %H:%M:%S'))
print("Time taken is %d days %d hours %d minutes %d seconds" % (diff.days, diff.hours, diff.minutes, diff.seconds))
