# import os
# import socket
# import time
# import random
# import datetime

# # Configurações do cliente
# CLIENT_ID = os.getenv("CLIENT_ID", f"Cliente_{random.randint(1000, 9999)}")
# CLUSTER_SYNC_HOST = os.getenv("CLUSTER_SYNC_HOST", '127.0.0.1')
# CLUSTER_SYNC_PORT = int(os.getenv("CLUSTER_SYNC_PORT", 12345))

# def iniciar_cliente():
#     print(f"[{CLIENT_ID}]: Iniciando e conectando a {CLUSTER_SYNC_HOST}:{CLUSTER_SYNC_PORT}...")
#     num_acessos = random.randint(10, 50)
#     print(f"{CLIENT_ID}: Fará {num_acessos} acessos ao Recurso R.")

#     for i in range(num_acessos):
#         timestamp = datetime.datetime.now().isoformat()
#         solicitacao = f"REQUEST|{CLIENT_ID}|{timestamp}"

#         try:
#             with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
#                 s.connect((CLUSTER_SYNC_HOST, CLUSTER_SYNC_PORT))
#                 s.sendall(solicitacao.encode('utf-8'))
#                 print(f"{CLIENT_ID}: Enviou solicitação {i+1} com timestamp {timestamp}.")

#                 data = s.recv(1024).decode('utf-8')
#                 if data == "COMMITTED":
#                     print(f"{CLIENT_ID}: Recebeu COMMITTED para solicitação {i+1}.")
#                     sleep_time = random.uniform(1, 5)
#                     print(f"{CLIENT_ID}: Dormindo por {sleep_time:.2f} segundos...")
#                     time.sleep(sleep_time)
#                 else:
#                     print(f"{CLIENT_ID}: Erro - Recebeu: {data}")

#         except ConnectionRefusedError:
#             print(f"{CLIENT_ID}: Erro: Conexão com o Cluster Sync recusada. Verifique se o servidor está rodando.")
#             break
#         except Exception as e:
#             print(f"{CLIENT_ID}: Ocorreu um erro: {e}")
#             break

#     print(f"{CLIENT_ID}: Finalizado após {num_acessos} acessos.")

# if __name__ == "__main__":
#     iniciar_cliente()































import socket
import time
import random
import datetime
import json # Adicione esta importação para trabalhar com JSON
import os

# --- Configurações do cliente ---
# Lendo as variáveis de ambiente
CLIENT_ID = os.getenv("CLIENT_ID", f"Cliente_{random.randint(1000, 9999)}")
CLUSTER_SYNC_HOST = os.getenv("CLUSTER_SYNC_HOST", '127.0.0.1')
CLUSTER_SYNC_PORT = int(os.getenv("CLUSTER_SYNC_PORT", 12345))

def iniciar_cliente():
    print(f"[{CLIENT_ID}]: Iniciando e conectando a {CLUSTER_SYNC_HOST}:{CLUSTER_SYNC_PORT}...")
    num_acessos = random.randint(10, 50)
    print(f"[{CLIENT_ID}]: Fará {num_acessos} acessos ao Recurso R.")

    for i in range(num_acessos):
        timestamp = datetime.datetime.now().isoformat()
        
        # Agora envia a requisição em formato JSON
        solicitacao_json = {
            "type": "REQUEST",
            "client_id": CLIENT_ID,
            "timestamp": timestamp
        }

        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(10) # Define um timeout para a conexão e recebimento
                s.connect((CLUSTER_SYNC_HOST, CLUSTER_SYNC_PORT))
                s.sendall(json.dumps(solicitacao_json).encode('utf-8')) # Envia JSON
                print(f"[{CLIENT_ID}]: Enviou solicitação {i+1} com timestamp {timestamp}.")

                data = s.recv(1024).decode('utf-8')
                if data == "COMMITTED":
                    print(f"[{CLIENT_ID}]: Recebeu COMMITTED para solicitação {i+1}.")
                    sleep_time = random.uniform(1, 5)
                    print(f"[{CLIENT_ID}]: Dormindo por {sleep_time:.2f} segundos...")
                    time.sleep(sleep_time)
                else:
                    # Tenta decodificar se for JSON (para depuração) ou mostra a raw data
                    try:
                        response_msg = json.loads(data)
                        print(f"[{CLIENT_ID}]: Erro - Recebeu mensagem inesperada: {response_msg}")
                    except json.JSONDecodeError:
                        print(f"[{CLIENT_ID}]: Erro - Recebeu resposta inválida: '{data}'")
                        if not data: # Se a resposta for vazia, significa que o servidor fechou a conexão antes de responder
                            print(f"[{CLIENT_ID}]: Servidor fechou a conexão antes de enviar COMMITTED.")

        except ConnectionRefusedError:
            print(f"[{CLIENT_ID}]: Erro: Conexão com o Cluster Sync recusada. Verifique se o servidor está rodando e acessível.")
            break
        except socket.timeout:
            print(f"[{CLIENT_ID}]: Erro: Timeout de conexão ou leitura da resposta para solicitação {i+1}.")
            break
        except Exception as e:
            print(f"[{CLIENT_ID}]: Ocorreu um erro: {e}")
            break

    print(f"[{CLIENT_ID}]: Finalizado após {i+1 if 'i' in locals() else 0} acessos.")

if __name__ == "__main__":
    iniciar_cliente()