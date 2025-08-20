# cluster_store_app/Dockerfile

FROM python:3.9-slim

WORKDIR /app

# Copia apenas o arquivo de código
COPY cluster_store_node.py .

# Instala dependências, se houver (neste caso, não há)
# RUN pip install ...

# Comando para rodar a aplicação
CMD ["python", "cluster_store_node.py"]