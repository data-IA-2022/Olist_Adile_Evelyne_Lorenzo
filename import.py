import os
import pandas as pd
from sqlalchemy import create_engine
import psycopg2
import time

# Enregistrer l'heure de début
start_time = time.time()

# Créer une connexion à la base de données PostgreSQL
conn = psycopg2.connect(
    host="localhost",
    database="t",
    user="postgres",
    password="greta2023"
)

# Créer un moteur SQLAlchemy pour la connexion à la base de données PostgreSQL
engine = create_engine('postgresql+psycopg2://', creator=lambda: conn, echo=False)

# Chemin d'accès au dossier contenant les fichiers CSV
path = r'C:\Users\loren\Desktop\MOOC\MOOC_G4_Maud_Lorenzo\Olist_Adile_Evelyne_Lorenzo\archive'

# Parcourir tous les fichiers CSV dans le dossier et les insérer dans la base de données
for filename in os.listdir(path):
    if filename.endswith('.csv'):
        # Charger le fichier CSV dans un dataframe pandas
        # Spécifiez le type de données pour les colonnes customer_zip_code_prefix et geolocation_zip_code_prefix
        dtype = None
        if 'customers' in filename:
            dtype = {'customer_zip_code_prefix': str}
        elif 'geolocation' in filename:
            dtype = {'geolocation_zip_code_prefix': str}
        elif 'sellers' in filename:
            dtype = {'seller_zip_code_prefix': str}

        df = pd.read_csv(os.path.join(path, filename), dtype=dtype)
        # Supprimer les espaces de début et de fin des noms de colonnes
        df.columns = df.columns.str.strip()
        # Enlever le mot "dataset" à la fin du nom de la table
        table_name = filename[:-4].replace("_dataset", "")
        # Insérer les données dans la base de données PostgreSQL
        df.to_sql(table_name, engine, if_exists='replace', index=False)

        # Exécuter les requêtes SQL pour effectuer les modifications nécessaires à la base de données
with open('bdd.sql', 'r') as file:
    queries = file.read()

    query_list = queries.split(';')

    with conn.cursor() as cursor:
        for query in query_list:
            if query.strip():  # Ignore les chaînes vides ou constituées uniquement d'espaces
                cursor.execute(query)
                conn.commit()

# Enregistrer l'heure de fin
end_time = time.time()

# Calculer la différence entre les deux
duration = end_time - start_time

# Afficher le temps d'exécution en secondes
print(f"Durée d'exécution : {duration} secondes")