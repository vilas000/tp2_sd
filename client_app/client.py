import socket
import time
import random
import datetime
import json
import os
import sys # Para saída de erro robusta

# --- Configurações do Cliente ---
# Obtém o ID do cliente e o host/porta do Cluster Sync das variáveis de ambiente.
# Isso garante que o Docker Compose pode configurar cada cliente dinamicamente.
CLIENT_ID = os.getenv("CLIENT_ID", f"Cliente_{random.randint(1000, 9999)}")
CLUSTER_SYNC_HOST = os.getenv("CLUSTER_SYNC_HOST", '127.0.0.1')

try:
    CLUSTER_SYNC_PORT = int(os.getenv("CLUSTER_SYNC_PORT", "0"))
    if CLUSTER_SYNC_PORT == 0:
        # Fallback para desenvolvimento local se a porta não for definida no ambiente
        # Mas para Docker Compose, ela deve ser sempre definida.
        print(f"[{CLIENT_ID}] Aviso: CLUSTER_SYNC_PORT não definida no ambiente, usando porta padrão 12345.", file=sys.stderr)
        CLUSTER_SYNC_PORT = 12345
except (ValueError, TypeError):
    print(f"[{CLIENT_ID}] ERRO FATAL: Valor de CLUSTER_SYNC_PORT inválido no ambiente. Saindo.", file=sys.stderr)
    sys.exit(1)


def iniciar_cliente():
    print(f"[{CLIENT_ID}]: Iniciando e conectando a {CLUSTER_SYNC_HOST}:{CLUSTER_SYNC_PORT}...")
    
    initial_delay = random.uniform(5, 10)
    print(f"[{CLIENT_ID}]: Aguardando {initial_delay:.2f} segundos antes de enviar requisições...")
    time.sleep(initial_delay)

    num_acessos = 1 
    #num_acessos = random.randint(10, 50)  # Conforme especificação
    print(f"[{CLIENT_ID}]: Fará {num_acessos} acessos ao Recurso R.")

    acessos_realizados = 0
    for i in range(num_acessos):
        timestamp = datetime.datetime.now().isoformat()
        
        solicitacao_json = {
            "type": "REQUEST",
            "client_id": CLIENT_ID,
            "timestamp": timestamp
        }

        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(30)
                s.connect((CLUSTER_SYNC_HOST, CLUSTER_SYNC_PORT))
                s.sendall(json.dumps(solicitacao_json).encode('utf-8'))
                print(f"[{CLIENT_ID}]: Enviou solicitação {i+1} com timestamp {timestamp}.")

                data = s.recv(1024)
                if not data:
                    print(f"[{CLIENT_ID}]: Conexão fechada pelo servidor.")
                    continue
                    
                data = data.decode('utf-8')
                if data == "COMMITTED":
                    print(f"[{CLIENT_ID}]: Recebeu COMMITTED para solicitação {i+1}. Acesso concedido.")
                    acessos_realizados += 1
                    sleep_time = random.uniform(1, 5)
                    print(f"[{CLIENT_ID}]: Dormindo por {sleep_time:.2f} segundos...")
                    time.sleep(sleep_time)
                else:
                    try:
                        response_msg = json.loads(data)
                        print(f"[{CLIENT_ID}]: Erro - Recebeu mensagem inesperada: {response_msg}")
                    except json.JSONDecodeError:
                        print(f"[{CLIENT_ID}]: Erro - Recebeu resposta inválida: '{data}'")
            
        except ConnectionRefusedError:
            print(f"[{CLIENT_ID}]: Erro: Conexão com o Cluster Sync recusada. Verifique se o servidor está rodando e acessível.")
            break
        except socket.timeout:
            print(f"[{CLIENT_ID}]: Erro na solicitação {i+1}: timed out. Servidor não respondeu a tempo.")
            continue
        except Exception as e:
            print(f"[{CLIENT_ID}]: Ocorreu um erro inesperado: {e}")
            break

    print(f"[{CLIENT_ID}]: Finalizado. Total de {acessos_realizados} acessos concedidos ao Recurso R.")



if __name__ == "__main__":
    iniciar_cliente()

