# Usa uma imagem base Python leve
FROM python:3.9-slim-buster

# Define o diretório de trabalho dentro do contêiner
WORKDIR /app

# Copia o script do peer do Cluster Sync para o diretório de trabalho
COPY cluster_sync_peer.py .

# Instala quaisquer dependências
# RUN pip install alguma_biblioteca_aqui

# Comando para executar o script do peer
# Passamos PEER_ID e informações do cluster como variáveis de ambiente
CMD ["python", "cluster_sync_peer.py"]