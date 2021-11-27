from pyspark.sql import SparkSession
from flask_ngrok import run_with_ngrok 
from flask import Flask
from flask import jsonify

# pip install pyspark
# pip install flask-ngrok

spark=SparkSession.builder.appName('Dataframe').getOrCreate()

df_cards=spark.read.option('header','true').option('delimiter','|').csv('gs://bucket-bigdata-jrg/cards.csv',inferSchema=True)
df_weather=spark.read.option('header','true').option('delimiter',';').csv('gs://bucket-bigdata-jrg/weather.csv',inferSchema=True)

spark.conf.set('spark.sql.repl.eagerEval.enabled', True)

df_cards.createOrReplaceTempView("TCARDS")
df_weather.createOrReplaceTempView("TWEATHER")

#Consultas
'''
dfgastosSun = spark.sql("")
dfgastosSun
'''

dfsectores = spark.sql("SELECT SECTOR, AVG(IMPORTE) FROM TCARDS GROUP BY SECTOR;")
dfgastosMonth = spark.sql("SELECT MONTH(DIA) AS `MES`, ROUND(SUM(IMPORTE), 2) AS `IMPORTES` FROM TCARDS GROUP BY `MES` ORDER BY `MES`;")
dfgastosSummer = spark.sql("SELECT MONTH(DIA) AS `MES`, FRANJA_HORARIA, ROUND(SUM(IMPORTE), 2) AS `IMPORTE TOTAL` FROM TCARDS WHERE MONTH(DIA) BETWEEN 6 AND 8 GROUP BY MONTH(DIA), FRANJA_HORARIA ORDER BY `IMPORTE TOTAL` DESC LIMIT 10;")

'''
dfgastosRain = spark.sql("")
dfgastosXtrem = spark.sql("")
'''

app = Flask(__name__) 
run_with_ngrok(app) 

@app.route("/") 
def home(): 
    return "<h1>Pruebas de llamada de la API</h1>"

@app.route('/api/GastoHorarioVerano', methods=['GET'])
def gasto_verano():
    response = dfgastosSummer
    return jsonify(response)

@app.route('/api/GastoSector', methods=['GET'])
def gasto_sector():
    response = dfsectores
    return jsonify(response)

print("Lanzando servicio")
app.run()
print("Se ha acabado la ejecucion")