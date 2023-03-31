import psycopg2

# Connexion à la base de données PostgreSQL
conn = psycopg2.connect(
    host="localhost",
    database="olist_test",
    user="postgres",
    password="greta2023"
)

# Création d'un curseur pour exécuter des requêtes SQL
cursor = conn.cursor()

# Exemple : Exécuter une requête SQL pour afficher le contenu de la table "mytable"
cursor.execute("SELECT * FROM olist_customers_dataset")
rows = cursor.fetchall()
for row in rows:
    print(row)

# Fermeture de la connexion à la base de données PostgreSQL
conn.close()
