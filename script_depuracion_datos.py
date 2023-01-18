import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql.session import SparkSession
import re
import pyspark.sql.functions as f
import pandas as pd
import pycountry

conf = SparkConf().setAppName('scriptDatos')
sc = SparkContext(conf = conf)
spark = SparkSession(sc)

'''
#--------------------2000--------------------
#anio2000 = pd.io.stata.read_stata('country_partner_hsproduct4digit_year_2000.dta')
#anio2000.to_csv('2000TOTAL.csv')

df2000 = spark.read.option("header", "true").csv('2000TOTAL.csv')
df2000 = df2000.na.drop()
df2000 = df2000.select(df2000['hs_product_code'].alias('id_producto'), df2000['location_code'].alias('pais1'), df2000['partner_code'].alias('pais2'), df2000['export_value'].alias('valor_exportacion'), df2000['import_value'].alias('valor_importacion'))
df2000 = df2000.filter(df2000['id_producto'] == 2710)
#df2000.show()

df2000.toPandas().to_excel("2000FILTRADO.xlsx", index=False)


#--------------------2010--------------------
#anio2010 = pd.io.stata.read_stata('country_partner_hsproduct4digit_year_2010.dta')
#anio2010.to_csv('2010TOTAL.csv')

df2010 = spark.read.option("header", "true").csv('2010TOTAL.csv')
df2010 = df2010.na.drop()
df2010 = df2010.select(df2010['hs_product_code'].alias('id_producto'), df2010['location_code'].alias('pais1'), df2010['partner_code'].alias('pais2'), df2010['export_value'].alias('valor_exportacion'), df2010['import_value'].alias('valor_importacion'))
df2010 = df2010.filter(df2010['id_producto'] == 2710)
#df2010.show()

df2010.toPandas().to_excel("2010FILTRADO.xlsx", index=False)


#--------------------2020--------------------
#anio2020 = pd.io.stata.read_stata('country_partner_hsproduct4digit_year_2020.dta')
#anio2020.to_csv('2020TOTAL.csv')

df2020 = spark.read.option("header", "true").csv('2020TOTAL.csv')
df2020 = df2020.na.drop()
df2020 = df2020.select(df2020['hs_product_code'].alias('id_producto'), df2020['location_code'].alias('pais1'), df2020['partner_code'].alias('pais2'), df2020['export_value'].alias('valor_exportacion'), df2020['import_value'].alias('valor_importacion'))
df2020 = df2020.filter(df2020['id_producto'] == 2710)
#df2020.show()

df2020.toPandas().to_excel("2020FILTRADO.xlsx", index=False)
'''

def convert(pais):
    return pycountry.countries.get(pais).alpha_2

#--------------------6 DIGITOS--------------------

#--------------------2000--------------------
anio2000 = pd.io.stata.read_stata('country_partner_hsproduct6digit_year_2000.dta')
anio2000.to_csv('2000TOTAL.csv')

df2000 = spark.read.option("header", "true").csv('2000TOTAL.csv')
df2000 = df2000.na.drop()
df2000 = df2000.filter(df2000['hs_product_code'] == 270900)

df2000EXP = df2000.select(df2000['location_code'].alias('pais1'), df2000['partner_code'].alias('pais2'), df2000['export_value'].alias('valor_exportacion'))
df2000EXP = df2000EXP.filter(df2000EXP['valor_exportacion'] != 0)

paisesOrigen = df2000EXP.select(df2000EXP['pais1']).as[String].collect.toList
paisesDestino = df2000EXP.select(df2000EXP['pais2']).as[String].collect.toList

df2000EXP.toPandas().to_excel("2000FILTRADO.xlsx", index=False)


#--------------------2010--------------------
anio2010 = pd.io.stata.read_stata('country_partner_hsproduct6digit_year_2010.dta')
anio2010.to_csv('2010TOTAL.csv')

df2010 = spark.read.option("header", "true").csv('2010TOTAL.csv')
df2010 = df2010.na.drop()
df2010 = df2010.filter(df2010['hs_product_code'] == 270900)

df2010EXP = df2010.select(df2010['location_code'].alias('pais1'), df2010['partner_code'].alias('pais2'), df2010['export_value'].alias('valor_exportacion'))
df2010EXP = df2010EXP.filter(df2010EXP['valor_exportacion'] != 0)

df2010EXP.toPandas().to_excel("2010FILTRADO.xlsx", index=False)


#--------------------2020--------------------
anio2020 = pd.io.stata.read_stata('country_partner_hsproduct6digit_year_2020.dta')
anio2020.to_csv('2020TOTAL.csv')

df2020 = spark.read.option("header", "true").csv('2020TOTAL.csv')
df2020 = df2020.na.drop()
df2020 = df2020.filter(df2020['hs_product_code'] == 270900)

df2020EXP = df2020.select(df2020['location_code'].alias('pais1'), df2020['partner_code'].alias('pais2'), df2020['export_value'].alias('valor_exportacion'))
df2020EXP = df2020EXP.filter(df2020EXP['valor_exportacion'] != 0)

df2020EXP.toPandas().to_excel("2020FILTRADO.xlsx", index=False)

print('--------------------SUCCESS--------------------')