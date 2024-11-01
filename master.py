
import pandas as pd
import hashlib
import time
from datetime import datetime
import os
import csv
import boto3
import os
#------------------------------------------------------------------------
csv_filepath = 'data-common-2024_11_01.csv'
#-------------------------------------------------------------------------
def master():
    def gerar_colunas_finais():
        df = pd.read_csv(csv_filepath)
        colunas_existentes = [
            'credits', 'name', 'message', 'time', 'formulario', 'contactAmount', 'classification', 'confidence'
        ]
        novas_colunas = [
            'pk', 'sk', 'typename', 'source', 'target', 'documentId', 'documentIndex', 'hash', 
            'type', 'root', 'classes', 'status', 'analysed', 'isOpportunity', 
            'markedAs', 'score', 'url', 'ttl'
        ]
        for coluna in novas_colunas:
            if coluna not in df.columns:
                df[coluna] = None  
        df.to_csv(csv_filepath, index=False)
    gerar_colunas_finais()
    #-------------------------------------------------------------------------
    def hash_document(content):
        return hashlib.sha256(content.encode('utf-8')).hexdigest()
    #-------------------------------------------------------------------------
    def processamento_inicial():
        df = pd.read_csv(csv_filepath)
        df['classification'] = df['classification'].fillna('not_analyzed')
        df['sk'] = df.apply(lambda x: f"Scrapers#{hash_document(x['message'])}" if pd.isna(x['sk']) or x['sk'] == '' else x['sk'], axis=1)
        df['typename'] = 'Scrapers'
        df['hash'] = df['message'].apply(hash_document)
        df['analysed'] = df['pk'].notna()
        df['isOpportunity'] = df['classification'] == 'OPORTUNIDADE'
        df['credits'] = df['credits'].fillna('N/A')
        df['name'] = df['name'].fillna('N/A')
        df['message'] = df['message'].fillna('N/A')
        df['confidence'] = df['confidence'].fillna(0.0)
        df['source'] = df['credits'].apply(lambda x: f"Scrapers#{x}")
        df['target'] = df.apply(lambda x: f"{x['credits']}#{x.name}", axis=1)
        df['documentId'] = df['credits']
        df['documentIndex'] = df.index
        df['type'] = 'JUSBRASIL'
        df['root'] = df['message']
        df['classes'] = df['classification']
        df['status'] = 'IN_ANALYSIS'
        df['markedAs'] = df['classification']
        df['score'] = df['confidence'].apply(lambda x: float(x) if isinstance(x, (int, float)) else 0.0)
        df['url'] = 'URL_EXEMPLO'
        df['ttl'] = int(time.time()) + 30 * 24 * 60 * 60  # 30 dias a partir de agora
    #-------------------------------------------------------------------------
        def formatar_data(original_time):
            if pd.isna(original_time):
                return 'N/A'
            if 'Enviada hoje às' in original_time:
                time_part = original_time.split('às')[-1].strip()
                today = datetime.now()
                return today.strftime(f"{time_part}, %d/%m/%Y")
            return original_time
        df['time'] = df['time'].apply(formatar_data)
        df.to_csv(csv_filepath, index=False)
    processamento_inicial()
    #-------------------------------------------------------------------------
    print("Arquivo CSV atualizado com sucesso.")
    #-------------------------------------------------------------------------
    #########################################################################
    # FASE 2
    ##########################################################################

    def fill_pk():
        df = pd.read_csv(csv_filepath)
        df['pk'] = 'Scrapers#JUSGRASIL'
        df.to_csv(csv_filepath, index=False)
        print("Coluna 'pk' preenchida com sucesso.")
    fill_pk()
    def pk():
        df = pd.read_csv(csv_filepath)
        if 'pk' in df.columns:
            def formatar_pk(valor):
                try:
                    data_formatada = datetime.strptime(valor, '%d_%m_%Y')
                    return f"scrapping_{data_formatada.strftime('%d_%m_%Y')}"
                except ValueError:
                    return valor
            df['pk'] = df['pk'].apply(formatar_pk)
            df.to_csv(csv_filepath, index=False)
            print("Atualização concluída com sucesso!")
        else:
            print("A coluna 'pk' não foi encontrada no arquivo CSV.")
    pk()    
    #-------------------------------------------------------------------------
    def source():
        df = pd.read_csv(csv_filepath)
        data_atual = datetime.now().strftime("%d_%m_%Y")
        df['source'] = f"scrapping_jus_{data_atual}_doc_index"
        df.to_csv(csv_filepath, index=False)
    source()
#-------------------------------------------------------------------------
    def atualizar_target():
        df = pd.read_csv(csv_filepath)
        data_atual = datetime.now().strftime("%d_%m_%Y")
        df['target'] = [f"doc_{i+1}_{data_atual}" for i in range(len(df))]
        df.to_csv(csv_filepath, index=False)
    atualizar_target()
    #---------------------------------------------------------------------     
    def fix_horarios():
        df = pd.read_csv(csv_filepath)
        horarios = df['time'].str.extract(r'(\d{2}:\d{2})')[0]
        data_atual = datetime.now().strftime('%d/%m/%Y')
        df['time'] = horarios.apply(lambda x: f"{data_atual} {x}" if pd.notnull(x) else None)
        print("Conteúdo atualizado da coluna 'time':")
        print(df['time'].head(10))  
        df.to_csv(csv_filepath, index=False)
        print("\nColuna 'time' substituída com sucesso!")
    fix_horarios()
    #---------------------------------------------------------------------
    def analyzed():
        df = pd.read_csv(csv_filepath)
        for index, row in df.iterrows():
            if row['status'] == 'IN_ANALYSIS':
                df.at[index, 'analysed'] = False
            elif row['status'] == 'COMPLETED':
                df.at[index, 'analysed'] = True
        df.to_csv(csv_filepath, index=False)
        print("Arquivo atualizado com sucesso.")
    analyzed()
    #---------------------------------------------------------------------
    def marked_as():
        df = pd.read_csv(csv_filepath)
        for index, row in df.iterrows():
            if row['markedAs'] == 'not_analyzed':
                df.at[index, 'markedAs'] = 'NAO_OPORTUNIDADE'
        df.to_csv(csv_filepath, index=False)
        print("Arquivo atualizado com sucesso.")
    marked_as()
    #---------------------------------------------------------------------
    def arredondar_score():
        import math
        df = pd.read_csv(csv_filepath)
        df['score'] = df['score'].apply(lambda x: math.ceil(x))
        df.to_csv(csv_filepath, index=False)
        print("Arredondamento concluído!")
    arredondar_score()
    #---------------------------------------------------------------------
    def url_ex():
        df = pd.read_csv(csv_filepath)
        for index, row in df.iterrows():
            if row['url'] == 'URL_EXEMPLO':
                df.at[index, 'url'] = ''
        df.to_csv(csv_filepath, index=False)
        print("Arquivo atualizado com sucesso.")
    url_ex()
    #---------------------------------------------------------------------
    def processar_document_id():
        df = pd.read_csv(csv_filepath)
        data_atual = datetime.now().strftime('%d_%m')
        def gerar_document_id(row):
            if 'gratuito' in row['credits'].lower():
                credits = '0C'
            else:
                credits = f"{int(row['credits'].split()[0])}C"
            indice = row['documentIndex']
            document_id = f"{row['name']}_{credits}_{data_atual}_{indice}"
            return document_id
        df['documentId'] = df.apply(gerar_document_id, axis=1)
        df.to_csv(csv_filepath, index=False)
    processar_document_id()
    #---------------------------------------------------------------------
    def confidence():
        df = pd.read_csv(csv_filepath)
        if 'confidence' not in df.columns:
            print("A coluna 'confidence' não foi encontrada.")
            return
        df['score'] = df['confidence'].str.replace('%', '').astype(float)
        df.to_csv(csv_filepath, index=False)
        print("Coluna 'score' atualizada com sucesso.")
    confidence()
    #---------------------------------------------------------------------
    def score():
        df = pd.read_csv(csv_filepath)
        if 'score' not in df.columns:
            print("A coluna 'score' não foi encontrada.")
            return
        try:
            df.loc[df['score'] > 0, 'analysed'] = True
            df.loc[df['score'] > 0, 'status'] = 'COMPLETED'
        except:
            print("Erro ao atualizar as colunas 'analysed' e 'status'.")
            pass
        df.to_csv(csv_filepath, index=False)
        print("Colunas 'analysed' e 'status' atualizadas com sucesso.")
    score()
    #---------------------------------------------------------------------
    def arredondar_score():
        import math
        df = pd.read_csv(csv_filepath)
        df['score'] = df['score'].apply(lambda x: math.ceil(x))
        df.to_csv(csv_filepath, index=False)
        print("Arredondamento concluído!")
    arredondar_score()
    #---------------------------------------------------------------------
    def bounce():
        df = pd.read_csv(csv_filepath)
        starter_columns = ["credits", "name", "message", "formulario", "contactAmount", "time", "classification", "confidence"]
        df_starter = df[starter_columns]
        starter_filepath = csv_filepath
        df_starter.to_csv(starter_filepath, index=False)
        df_master = df.drop(columns=starter_columns)
        master_filepath = f"{os.path.splitext(csv_filepath)[0]}_master.csv"
        df_master.to_csv(master_filepath, index=False)
        print (f"Cópias criadas: {starter_filepath} e {master_filepath}")
    bounce()
    #---------------------------------------------------------------------
    def depoy():
        dynamodb = boto3.resource('dynamodb')
        table_name = 'alf-BigTable-dev'
        table = dynamodb.Table(table_name)
        csv_filepath_master = os.path.splitext(csv_filepath)[0] + '_master.csv'
        with open(csv_filepath_master, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                item = {
                    'pk': row['pk'],
                    'sk': row['sk'],
                    'typename': row['typename'],
                    'source': row['source'],
                    'target': row['target'],
                    'documentId': row['documentId'],
                    'documentIndex': int(row['documentIndex']),
                    'hash': row['hash'],
                    'type': row['type'],
                    'root': row['root'],
                    'classes': row.get('classes', ''),
                    'status': row['status'],
                    'analysed': row['analysed'].lower() == 'true',
                    'isOpportunity': row['isOpportunity'].lower() == 'true',
                    'markedAs': row.get('markedAs', ''),
                    'score': int(row['score']) if row['score'] else 0,
                    'url': row.get('url', ''),
                    'ttl': int(row['ttl'])  
                }
                try:
                    table.put_item(Item=item)
                    print(f"Item {item['documentId']} adicionado com sucesso.")
                except Exception as e:
                    print(f"Erro ao adicionar item {item['documentId']}: {e}")
    depoy()
#-------------------------------------------------------------------------
master()