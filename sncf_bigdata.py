from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc

# Création de la session Spark
spark = SparkSession.builder \
    .appName("SNCF Objets Trouvés") \
    .getOrCreate()

# Vérification
print("Session Spark démarrée avec succès :", spark.version)

# Lecture du CSV
df = spark.read.option("header", "true").option("sep", ";").option("encoding", "utf-8").csv("objets-trouves-restitution.csv")

df.printSchema()

print("Top 5 des gares avec le plus d’objets trouvés :")

top_gares = df.groupBy("gare").count().orderBy(desc("count")).limit(5)
top_gares.show()

print("Nature d’objet la plus fréquemment retrouvée (toutes gares confondues) :")

top_objets = df.groupBy("Nature d'objets").count().orderBy(desc("count")).limit(1)
top_objets.show()
