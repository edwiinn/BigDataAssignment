#!/usr/bin/env python
# coding: utf-8

# In[4]:


import os
from pyspark.sql import functions as F
import pyspark.sql.types as T
from pyspark.mllib.recommendation import ALS
from pyspark.sql import Row
from pyspark.ml.fpm import FPGrowth

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def unique(list1): 
  
    # intilize a null list 
    unique_list = [] 
      
    # traverse for all elements 
    for x in list1:
        x = x.strip()
        # check if exists in unique_list or not 
        if x not in unique_list: 
            unique_list.append(x)
      
    return unique_list
def fudf(val):
    return unique(val)

flattenUdf = F.udf(fudf, T.ArrayType(T.StringType()))

class RecommendationEngine:
    def get_user(self,user_id):
        available = self.groupedUserProductDf.filter(self.groupedUserProductDf["user_id"] == user_id).collect()
        if len(available) == 0:
            return None
        return available[0]
    
    def get_user_products(self, user_id):
        """Get All of User Products
        """
        available = self.groupedUserProductDf.filter(self.groupedUserProductDf["user_id"] == user_id).collect()
        if len(available) == 0:
            return None
        return available[0][1]
    
    def add_user(self,inUsers_id,products):
        new_user = [Row(user_id=inUsers_id,ListItem=products)]
        new_user = self.sc.createDataFrame(new_user)
        new_user = new_user.select("user_id", flattenUdf("ListItem").alias("ListItem"))
        self.groupedUserProductDf = self.groupedUserProductDf.union(new_user)
        logger.info("User Added")
        #self.__train_model()
        return new_user.collect()[0]
        
    def get_user_reccomendation(self,user_id):
        """Get reccomendation for user
        """
        products = self.get_user_products(user_id)
        if products == None:
            return None
        temp = [Row(ListItem=products)]
        temp = self.sc.createDataFrame(temp)
        reccomendation = self.model.transform(temp)
        if reccomendation == None:
            return None
        return reccomendation.collect()[0]
        
    def __train_model(self):
        logger.info("Training Dataset")
        fpGrowth = FPGrowth(itemsCol="ListItem", minSupport=0.004, minConfidence=0.05)
        self.model = fpGrowth.fit(self.groupedUserProductDf)
    
    def __init__(self,sc,dataset_path):
        logger.info("Starting Up Reccomendation Engine: ")
        self.sc = sc
        logger.info("Loading Dataset")
        df = self.sc.read.format("csv").option("header", "true").option("mode", "DROPMALFORMED").load(dataset_path)
        
        userProductDf = df[["user_id","product_id"]]
        self.groupedUserProductDf = userProductDf.groupBy('user_id').agg(F.collect_list('product_id').alias("ListItem"))
        self.groupedUserProductDf = self.groupedUserProductDf.select("user_id", flattenUdf("ListItem").alias("ListItem"))
        
        self.__train_model()
        

