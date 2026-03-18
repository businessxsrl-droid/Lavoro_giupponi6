# Usa un'immagine Python ufficiale
FROM python:3.11-slim

# Imposta la directory di lavoro
WORKDIR /app

# Installa le dipendenze di sistema necessarie per psycopg2 e altre librerie
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copia i file dei requisiti
COPY requirements.txt .

# Installa le dipendenze Python
RUN pip install --no-cache-dir -r requirements.txt

# Copia tutto il codice del progetto
COPY . .

# Espone la porta dell'app
EXPOSE 5000

# Comando di avvio (usa Gunicorn come in produzione o Flask dev per locale)
CMD ["python", "app.py"]
