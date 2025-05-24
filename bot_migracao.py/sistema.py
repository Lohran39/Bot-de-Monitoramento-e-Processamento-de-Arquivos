import time
import os
import pandas as pd
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import requests

# Define pastas
pasta_entrada = 'entrada'
pasta_saida = 'saida'
log_file = 'log.txt'

# Função para escrever log (com encoding utf-8 para emojis)
def escrever_log(mensagem):
    timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
    with open(log_file, 'a', encoding='utf-8') as f:
        f.write(f'[{timestamp}] {mensagem}\n')
    print(mensagem)

# Função de envio de aviso pelo Telegram (mensagem + arquivo)
def enviar_aviso(nome_arquivo, caminho_arquivo):
    token = '8078687900:AAFw-EBJe74NJfC05C_R8bV9ZBtlI-TpBoA'
    chat_id = '1888038150'
    mensagem = f'✅ Arquivo {nome_arquivo} processado com sucesso!'

    url_mensagem = f'https://api.telegram.org/bot{token}/sendMessage'
    params = {'chat_id': chat_id, 'text': mensagem}
    try:
        response = requests.get(url_mensagem, params=params, timeout=10)
        if response.status_code == 200:
            escrever_log('✅ Mensagem enviada pelo Telegram!')
        else:
            escrever_log(f'❌ Erro ao enviar mensagem: {response.text}')
    except Exception as e:
        escrever_log(f'❌ Exceção no envio da mensagem: {e}')

    # Envia o arquivo como anexo
    url_documento = f'https://api.telegram.org/bot{token}/sendDocument'
    try:
        with open(caminho_arquivo, 'rb') as arquivo:
            files = {'document': arquivo}
            data = {'chat_id': chat_id}
            response = requests.post(url_documento, data=data, files=files, timeout=30)
        if response.status_code == 200:
            escrever_log('✅ Arquivo enviado pelo Telegram!')
        else:
            escrever_log(f'❌ Erro ao enviar anexo: {response.text}')
    except Exception as e:
        escrever_log(f'❌ Exceção no envio do arquivo: {e}')

# Função de processamento do arquivo Excel
def processar_arquivo(caminho_entrada):
    nome_arquivo = os.path.basename(caminho_entrada)
    escrever_log(f'🔄 Processando: {nome_arquivo}')

    try:
        # Pequena pausa para garantir que o arquivo está pronto
        time.sleep(0.5)
        
        # Lê o Excel (com engine openpyxl)
        df = pd.read_excel(caminho_entrada, index_col=None, engine='openpyxl')

        # Transformações básicas
        df['Nome'] = df['Nome'].str.upper()
        df['Data de Admissão'] = pd.to_datetime(df['Data de Admissão']).dt.strftime('%Y-%m-%d')

        # Caminho do arquivo de saída
        caminho_saida = os.path.join(pasta_saida, f'novo_{nome_arquivo}')

        # Salva com engine xlsxwriter para evitar erros de estilo
        df.to_excel(caminho_saida, index=False, engine='xlsxwriter')
        escrever_log(f'✅ Arquivo salvo em: {caminho_saida}')

        # Envia aviso e arquivo pelo Telegram
        enviar_aviso(nome_arquivo, caminho_saida)

    except Exception as e:
        escrever_log(f'❌ Erro ao processar {nome_arquivo}: {e}')

# Classe para monitorar criação de novos arquivos na pasta de entrada
class MonitorHandler(FileSystemEventHandler):
    def on_created(self, event):
        if event.is_directory:
            return
        if event.src_path.endswith('.xlsx'):
            escrever_log(f'📥 Arquivo detectado: {os.path.basename(event.src_path)}')
            time.sleep(1)  # espera o arquivo terminar de ser copiado
            processar_arquivo(event.src_path)

# Cria pastas se não existirem
os.makedirs(pasta_entrada, exist_ok=True)
os.makedirs(pasta_saida, exist_ok=True)

# Inicia monitoramento da pasta
observer = Observer()
event_handler = MonitorHandler()
observer.schedule(event_handler, path=pasta_entrada, recursive=False)
observer.start()

escrever_log(f'Bot rodando... monitorando a pasta "{pasta_entrada}"')

try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    observer.stop()
    escrever_log('🛑 Bot interrompido manualmente.')

observer.join()
