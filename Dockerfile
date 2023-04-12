FROM tiangolo/uvicorn-gunicorn-fastapi:python3.8

# Définition du répertoire de travail de l'application
WORKDIR /app

# Copier les fichiers de l'application dans le conteneur
COPY . /app

# Installation des dépendances
RUN pip install -r requirements.txt

# Définition des variables d'environnement
ENV OLIST=postgresql://writer:***@127.0.0.1/olist

# Exposition du port de l'application
EXPOSE 80

# Commande pour démarrer l'application
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "80"]