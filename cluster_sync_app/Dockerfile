# Imagem base Python leve
FROM python:3.9-slim-buster

# Diretório de trabalho dentro do contêiner
WORKDIR /app

# Copiar o script do peer do Cluster Sync para o diretório de trabalho
COPY cluster_sync_peer.py .

# Executar o script do peer
CMD ["python", "cluster_sync_peer.py"]