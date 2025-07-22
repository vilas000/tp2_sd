# Usa uma imagem base Python leve
FROM python:3.9-slim-buster

# Define o diretório de trabalho dentro do contêiner
WORKDIR /app

# Copia o script do cliente para o diretório de trabalho
COPY client.py .

# Instala quaisquer dependências (se o seu client.py tivesse alguma, como requests)
# RUN pip install alguma_biblioteca_aqui

# Comando para executar o script do cliente
# Passamos CLIENT_ID e CLUSTER_SYNC_HOST/PORT como variáveis de ambiente
CMD ["python", "client.py"]