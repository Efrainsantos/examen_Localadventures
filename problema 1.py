# -*- coding: utf-8 -*-
"""
Created on Sat Jul 11 20:30:56 2020

@author: Efrain Santos Luna
"""


import mysql.connector
import pandas as pd
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import col,asc,when,desc,lit

##creamos el contexto de apache spark
sc = SparkContext()

#creamos el contexto de sql
sqlContext = SQLContext(sc)

#conexion a la base de datos para consultas
mydb = mysql.connector.connect(
  host = 'localhost',
  port = 3306,
  user = 'root',
  passwd = 'Calculoefrain1',
  database = 'lahman2016',
  auth_plugin='mysql_native_password'
)


'''punto 1'''

def calcula_promedio(tabla_referencia,tabla_datos):
    '''esta funcion se encarga de calcular el salario promedio agrupando por el anio'''
    #se realiza la consulta a la BD y se lee con pandas
    query = 'SELECT ' + tabla_referencia + '.' + col_join + ',' + tabla_datos + '.salary,' + tabla_datos  + '.yearID FROM '  +\
    tabla_referencia + ' INNER JOIN '  + tabla_datos + ' ON ' + tabla_referencia + '.' + col_join + '=' + tabla_datos + '.' + col_join
    salarios = pd.read_sql(query,mydb)
    #se crea el dataframe de spark con el contextsql
    data_frame = sqlContext.createDataFrame(salarios)
    #se calcula el salario, se agrupa y se ordena
    media = data_frame.distinct().groupBy('yearID').mean('salary')
    media = media.sort(asc("yearID"))
    return media

#columna y tablas
col_join = 'playerID'
tabla_referencia = 'pitching'
tabla_datos = 'salaries'
#salario de los pitching
salarios_pitching = calcula_promedio(tabla_referencia,tabla_datos)

tabla_referencia = 'fielding'
tabla_datos = 'salaries'
#salario de los fielding
salarios_fielding = calcula_promedio(tabla_referencia,tabla_datos)

#unimos y ordenamos los dos dataframes
Average_Salaries = salarios_fielding.join(salarios_pitching, on=['yearID'], how='left_outer').sort(desc("yearID"))
#convertimos a un dataframe de pandas y cambiamos el nombre de las columnas
Average_Salaries = Average_Salaries.toPandas()

Average_Salaries.columns = ['yearID','Fielding','Pitching']

Average_Salaries.to_csv('Average Salaries.csv', index=False)


'''punto 2'''

#All Star Appearance
tabla_referencia = 'pitching'
tabla_datos = 'halloffame'

col_join = 'playerID'

query = 'SELECT halloffame.playerID FROM halloffame INNER JOIN pitching ON (pitching.playerID=halloffame.playerID)'
pitch_salon_fama = pd.read_sql(query,mydb)
data_frame1 = sqlContext.createDataFrame(pitch_salon_fama)
df1 = data_frame1.distinct()

tabla_datos = 'allstarfull'
query = 'SELECT ' + col_join + ',yearID, gameNUM FROM '  + tabla_datos
juegos_estrella = pd.read_sql(query,mydb)
data_frame2 = sqlContext.createDataFrame(juegos_estrella)
df2 = data_frame2.distinct()

df3 = df1.join(df2, on=['playerID'], how='left_outer').sort(asc("playerID"))

df3 = df3.withColumn("gameNUM",when(df3["gameNUM"] == 0, 1).otherwise(df3["gameNUM"]))
## 
All_Star_Appearance = df3.groupBy('playerID').sum('gameNUM')


#cuando se incluyo en el salon de la fama

query = 'SELECT halloffame.playerID,halloffame.yearid FROM halloffame INNER JOIN pitching ON (pitching.playerID=halloffame.playerID)'
pitch_salon_fama = pd.read_sql(query,mydb)
data_frame4 = sqlContext.createDataFrame(pitch_salon_fama)
df4 = data_frame4.distinct()
anios_ingreso = df4.groupBy('playerID').min('yearid').sort(asc("playerID"))


induction_year = anios_ingreso.join(All_Star_Appearance, on=['playerID'], how='left_outer').sort(asc("playerID"))


#ERA
#extramos los primeros datos de la BD correspondientes a pitching que estan en el salon de la fama
query = 'SELECT pitching.playerID,pitching.yearid,pitching.IPOuts,pitching.R FROM pitching INNER JOIN halloffame ON (pitching.playerID=halloffame.playerID)'
carreras = pd.read_sql(query,mydb)
data_frame5 = sqlContext.createDataFrame(carreras)
df5 = data_frame5.distinct()

#extraemos los jugadores estrellas
tabla_datos = 'allstarfull'
query = 'SELECT ' + col_join + ',yearID FROM '  + tabla_datos
juegos_estrella = pd.read_sql(query,mydb)
data_frame2 = sqlContext.createDataFrame(juegos_estrella)
df2 = data_frame2.distinct()

#juntamos ambos dataframes 
df3 = df5.join(df2, on=['playerID'], how='left_outer').sort(asc("playerID"))

#calculamos el ERA, la formula dice que se multiplica por 9 pero pero un IPOuts es igual a 3 innings pitched por eso se mulplica por 27
df3= df3.withColumn(
  'operacion',
  lit(27) * col('R') / col('IPOuts')
)

#agrupamos por id de jugador
era = df3.groupBy('playerID').mean('operacion')



#juntamos todos los data frame
Pitchers = era.join(induction_year, on=['playerID'], how='left_outer')
#reordenamos las columnas
Pitchers = Pitchers.select("playerID","avg(operacion)","sum(gameNUM)","min(yearid)")

#convertimos a un dataframe de pandas y cambiamos el nombre de las columnas
Pitchers = Pitchers.toPandas()
Pitchers.columns = ['Player','ERA','# All Star Appearances','Hall of Fame Induction Year']
Pitchers = Pitchers.fillna('-')
#guardamos en un csv
Pitchers.to_csv('Hall of Fame All Star Pitchers.csv', index=False)

'''punto 3'''


'''punto 4'''

query = 'select yearID,teamID,teams.Rank,AB from teams'
equipos = pd.read_sql(query,mydb)
equipos_df = sqlContext.createDataFrame(equipos)

#ordenar por las year, rank y AB no tiene sentido ya que la ordenacion esta dada por el year y el ranking
equipos_df = equipos_df.orderBy('yearID','Rank', ascending=True)
equipos_df.toPandas().to_csv('Rankings.csv', index=False)

