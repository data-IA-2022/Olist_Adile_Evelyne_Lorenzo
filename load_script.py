import os
import pandas as pd
from sqlalchemy import create_engine

# Créer une connexion à la base de données PostgreSQL
import psycopg2
conn = psycopg2.connect(
    host="localhost",
    database="olist_test",
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
        df = pd.read_csv(os.path.join(path, filename))
        # Supprimer les espaces de début et de fin des noms de colonnes
        df.columns = df.columns.str.strip()
        # Insérer les données dans la base de données SQLite
        df.to_sql(filename[:-4], engine, if_exists='replace', index=False)

# Fermer la connexion à la base de données
engine.dispose()

