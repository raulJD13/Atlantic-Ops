FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ /app/src/

# Exponemos el puerto de Streamlit
EXPOSE 8501

# Comando para lanzar la interfaz web
CMD ["streamlit", "run", "src/streamlit/app.py", "--server.port=8501", "--server.address=0.0.0.0"]