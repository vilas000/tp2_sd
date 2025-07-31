import socket
import time
import random
import datetime
import json
import os
import sys 

# configuracoes iniciais do cliente
# CLIENT_ID eh o nome unico do cliente, pode ser definido via variavel de ambiente ou gerado aleatoriamente
# CLUSTER_SYNC_HOST e CLUSTER_SYNC_PORT definem o endereco do no do cluster sync com o qual esse cliente deve se comunicar
CLIENT_ID = os.getenv("CLIENT_ID", f"Cliente_{random.randint(1000, 9999)}")
CLUSTER_SYNC_HOST = os.getenv("CLUSTER_SYNC_HOST", '127.0.0.1')

try:
    CLUSTER_SYNC_PORT = int(os.getenv("CLUSTER_SYNC_PORT", "0"))
    if CLUSTER_SYNC_PORT == 0:
        # fallback para desenvolvimento local se a porta não for definida no ambiente
        # para Docker Compose, ela deve ser sempre definida.
        print(f"[{CLIENT_ID}] Aviso: CLUSTER_SYNC_PORT não definida no ambiente, usando porta padrão 12345.", file=sys.stderr)
        CLUSTER_SYNC_PORT = 12345
except (ValueError, TypeError):
    print(f"[{CLIENT_ID}] ERRO FATAL: Valor de CLUSTER_SYNC_PORT inválido no ambiente. Saindo.", file=sys.stderr)
    sys.exit(1)


def iniciar_cliente():
    '''
    funcao com toda a logica de comportamento do cliente.
    '''
    print(f"[{CLIENT_ID}]: Iniciando e conectando a {CLUSTER_SYNC_HOST}:{CLUSTER_SYNC_PORT}...")
    
    # antes de comecar a enviar requisições, aguarda um tempo aleatório para que nem todos os 5 clientes enviem os primeiros pedidos ao mesmo tempo
    initial_delay = random.uniform(5, 10)
    print(f"[{CLIENT_ID}]: Aguardando {initial_delay:.2f} segundos antes de enviar requisições...")
    time.sleep(initial_delay)

    # cada cliente determina quantas vezes tentara acessar o recurso compartilhado aleatoriamente
    #num_acessos = 1 
    num_acessos = random.randint(10, 50)  
    print(f"[{CLIENT_ID}]: Fará {num_acessos} acessos ao Recurso R.")

    # aqui comeca o ciclo de vida do cliente, que se repetira ate que o numero de acessos desejado seja atingido
    acessos_realizados = 0
    for i in range(num_acessos):

        # gera um timestamp preciso para o momento, que o cluster ira usar para determinar a prioridade da requisicao
        timestamp = datetime.datetime.now().isoformat()
        
        solicitacao_json = {
            "type": "REQUEST",
            "client_id": CLIENT_ID,
            "timestamp": timestamp
        }

        # abre uma conexao com o peer, o cliente nao espera para sempre, se o algo falhar ou nenhuma resposta chegar em 30 segundos o cliente desiste e um erro eh lancado
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(30)
                s.connect((CLUSTER_SYNC_HOST, CLUSTER_SYNC_PORT))
                s.sendall(json.dumps(solicitacao_json).encode('utf-8'))
                print(f"[{CLIENT_ID}]: Enviou solicitação {i+1} com timestamp {timestamp}.")

                # espera pela resposta do servidor, que pode ser COMMITTED ou um erro
                data = s.recv(1024)
                if not data:
                    print(f"[{CLIENT_ID}]: Conexão fechada pelo servidor.")
                    continue
                    
                data = data.decode('utf-8')

                # verifica se a resposta eh COMMITED, caso de sucesso
                # usa o recurso e o loop continua para a proxima requisicao
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
