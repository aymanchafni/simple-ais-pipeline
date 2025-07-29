FROM python:3.11-slim

WORKDIR /app

# Installation des dépendances système
RUN apt-get update && apt-get install -y \
    gcc \
    postgresql-client \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Installation des dépendances Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copie du code source
COPY src/ ./src/
COPY scripts/ ./scripts/
COPY sample_data/ ./sample_data/

# Création des répertoires nécessaires
RUN mkdir -p data logs

# Script d'entrée
COPY scripts/docker_entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Variables d'environnement
ENV PYTHONPATH=/app

# Exposition du port
EXPOSE 8000
EXPOSE 8501  
EXPOSE 8080  
EXPOSE 5432

# Point d'entrée
#ENTRYPOINT ["/entrypoint.sh"]

# Commande par défaut
#CMD ["python", "-m", "uvicorn", "src.api.main:app", "--host", "0.0.0.0", "--port", "8000"]