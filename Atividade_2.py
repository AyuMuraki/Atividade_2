from dotenv import load_dotenv # manter informações sensíveis como senha do bd e url.
import os # funções para interagir com o sistema operacional
import pymysql # Biblioteca para conectar e interagir com um banco de dados MySQL.
from datetime import datetime # manipulação de datas e horas.
from typing import List
from playwright.sync_api import sync_playwright # coletar dados de páginas web.

# Carregar variáveis do arquivo .env
load_dotenv()

URL_BASE = os.getenv('url_base_path')

URLS = {
     "Brasil": f"{URL_BASE}brazil-indices?include-major-indices=true&include-additional-indices=true&include-primary-sectors=true&include-other-indices=true",
     "China": f"{URL_BASE}china-indices?include-primary-sectors=true",
     "EUA": f"{URL_BASE}usa-indices?include-primary-sectors=true"
}
#  Representa um setor econômico. Possui dois atributos: id e nome.
class Setor:
    def __init__(self, id: int, nome: str):
        self.id = id
        self.nome = nome
    def __repr__(self):
        return (f"Setor(id={self.id}, nome={self.nome}")  

class Pais:
    def __init__(self, id: int, nome: str): # Possui dois atributos: id e nome.
        self.id = id
        self.nome = nome
    def __repr__(self): # Define como a classe será representada como string.
        return (f"Pais(id={self.id}, nome={self.nome}")  

# Representa um índice financeiro. 
# Possui atributos como nome, valor_atual, maxima, minima, variacao e setor.
class Indice:
    def __init__(self,  nome: str, valor_atual: float, maxima: float, minima: float, variacao: float, setor: str):
        self.nome = nome
        self.valor_atual = valor_atual
        self.maxima = maxima
        self.minima = minima
        self.variacao = variacao
        self.setor = setor
    def __repr__(self):
        return (f"Indice(nome={self.nome}, valor_atual={self.valor_atual}, "
                f"maxima={self.maxima}, minima={self.minima}, "
                f"variacao={self.variacao}, setor={self.setor})")
 
# Executa uma consulta SQL para obter os 10 maiores índices dos países com ID 2 e 3 (China e EUA).
# Os resultados são convertidos em uma lista de objetos MaioresIndicesModel.  
class MaioresIndicesModel:
    def __init__(self, id: int, indice: str, pais: str, setor: str, 
                 valor_atual: float, maxima: float, minima: float, 
                 variacao: float, data_coleta: datetime):
        self.id = id
        self.indice = indice
        self.pais = pais
        self.setor = setor
        self.valor_atual = valor_atual
        self.maxima = maxima
        self.minima = minima
        self.variacao = variacao
        self.data_coleta = data_coleta
    def __repr__(self):
        return (f"MaioresIndicesModel(id={self.id}, indice={self.indice}, pais={self.pais}, "
                f"setor={self.setor}, valor_atual={self.valor_atual}, "
                f"maxima={self.maxima}, minima={self.minima}, "
                f"variacao={self.variacao}, data_coleta={self.data_coleta})")
          

conexao_bd = pymysql.connect(
    host=os.getenv('db_host'),
    user=os.getenv('db_user'),
    password=os.getenv('db_password'),
    database=os.getenv('db_name')
    )
cursor_bd = conexao_bd.cursor()

def obter_maiores_indices():
    try:
        cursor_bd.execute(f"SELECT I.ID, I.NOME as Indice, P.NOME as Pais, S.NOME as Setor, VALOR_ATUAL, MAXIMA, MINIMA, VARIACAO, DATA_COLETA "
                          f"FROM Indice I "
                          f"INNER JOIN Setor S ON I.setor_id = S.id "
                          f"INNER JOIN Pais P ON I.pais_id = P.id "
                          f"WHERE pais_id in (3, 2) "
                          f"ORDER BY MAXIMA DESC "
                          f"LIMIT 10;")
        # obtem linhas da projecao
        result_set: List[MaioresIndicesModel] = cursor_bd.fetchall()
        # converte para lista de MaioresIndicesModel
        valores = [MaioresIndicesModel(id=linha[0],
                                       indice=linha[1],
                                       pais=linha[2],
                                       setor=linha[3],
                                       valor_atual=linha[4],
                                       maxima=linha[5],
                                       minima=linha[6],
                                       variacao=linha[7],
                                       data_coleta=linha[8]) for linha in result_set]
        return valores
    except Exception as e:
        print(f"Erro ao inserir dados: {e}")
# Insere os dados coletados na tabela INDICE do banco de dados. Primeiro, filtra o país e o setor com base no nome, 
# e depois insere os dados de cada índice na tabela.
def inserir_dados_base(dados: List[Indice], setores: List[Setor], paises: List[Pais], pais: str):
    try:
        # Buscar o país pelo nome (acessando atributos) usando lambda
        pais_filtrado = [p for p in paises if p.nome.lower() == pais.lower()]
        
        if not pais_filtrado:
            print(f"País '{pais}' não encontrado.")
            return
        pais_d = pais_filtrado[0]
        
        # Definir o setor com base no país
        busca = 'Primário' if pais != "Brasil" else "Todos"
        setor_filtrado = [s for s in setores if s.nome.lower() == busca.lower()]
        if not setor_filtrado:
            print(f"Setor '{busca}' não encontrado.")
            return
        setor_d = setor_filtrado[0]
        
        # Preparar inserts com a data atual
        data_coleta = datetime.now().date()  # Data atual (apenas a data, sem horário)
        
        # preparar inserts
        for d in dados:
            cursor_bd.execute(
                "INSERT INTO INDICE (NOME, PAIS_ID, SETOR_ID, VALOR_ATUAL, MAXIMA, MINIMA, VARIACAO, DATA_COLETA) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                (d.nome, pais_d.id, setor_d.id, d.valor_atual, d.maxima, d.minima, d.variacao, data_coleta)
            )
            conexao_bd.commit()
    except Exception as e:
        print(f"Erro ao inserir dados: {e}")
        
# Buscar setores na tabela da base mysql
# Carrega todos os setores da tabela SETOR e os converte em uma lista de objetos Setor.
def carregar_setores_base():
    try:
        cursor_bd.execute("SELECT ID, NOME FROM SETOR")
        result_set: List[Setor] = cursor_bd.fetchall()
        # Convertendo as tuplas em instâncias da classe Setor
        setores = [Setor(id=linha[0], nome=linha[1]) for linha in result_set]
        return setores
    except (ValueError, AttributeError):
        return 0
# Buscar paises na tabela da base mysql
# Carrega todos os países da tabela PAIS e os converte em uma lista de objetos Pais.
def carregar_paises_base():
    try:
        cursor_bd.execute("SELECT ID, NOME FROM PAIS")
        result_set: List[Pais] = cursor_bd.fetchall()
         # Convertendo as tuplas em instâncias da classe Pais
        paises = [Pais(id=linha[0], nome=linha[1]) for linha in result_set]
        return paises
    except (ValueError, AttributeError):
        return 0
# Buscar dados na fonte (site)
# Usa o Playwright para navegar até a URL fornecida.
# extrair os dados da tabela de índices e retornar uma lista de objetos Indice.
def buscar_dados(url: str, pais: str):
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(url, wait_until='domcontentloaded', timeout=90000)

        resultado = []  # Inicializando a lista corretamente
        linhas = page.query_selector_all("tbody tr")
        for linha in linhas:
            colunas = linha.query_selector_all("td")
            if len(colunas) >= 6:
                try:
                    # Carregando os dados da fonte em variaveis
                    nome = (colunas[1].query_selector("a")).inner_text()
                    valor_atual = float((colunas[2].inner_text()).strip().replace(".", "").replace(",", "."))
                    maxima = float(( colunas[3].inner_text()).strip().replace(".", "").replace(",", "."))
                    minima = float(( colunas[4].inner_text()).strip().replace(".", "").replace(",", "."))
                    variacao = float(( colunas[5].inner_text()).strip().replace(".", "").replace(",", ".").replace("+", "").replace("%", ""))
                    setor = "Todos" if pais == "Brasil" else "Primario"

                    # Adicionando o objeto Indice à lista resultado
                    resultado.append(Indice(nome, valor_atual, maxima, minima, variacao, setor))
                except (ValueError, AttributeError):
                    print(f"Erro ao processar linha")
                    continue
        browser.close()
        return resultado

# Função principal que orquestra todo o processo:
def main():
    # carregar tabelas da base
    # Carrega setores e países do banco de dados.
    setores_lista = carregar_setores_base()
    paises = carregar_paises_base()
    # buscar dados da fonte
    # Coleta dados dos índices do Brasil, China e EUA.
    dados_brasil = buscar_dados(URLS["Brasil"], "Brasil")
    # inserir dados na base
    # Insere os dados coletados no banco de dados.
    inserir_dados_base(dados_brasil, setores_lista, paises, "Brasil")
    # buscar dados da fonte
    # Recupera e exibe os 10 maiores índices.
    dados_china = buscar_dados(URLS["China"],"China")
    # inserir dados na base
    inserir_dados_base(dados_china, setores_lista, paises, "China")
    # buscar dados da fonte
    dados_eua = buscar_dados(URLS["EUA"],"EUA")
    # inserir dados na base
    inserir_dados_base(dados_eua, setores_lista, paises, "EUA")
    # trazer 10 maiores indices de china e eua
    print(obter_maiores_indices())
    # fecha cursor e conecao com a base
    # Fecha a conexão com o banco de dados.
    cursor_bd.close()
    conexao_bd.close()

#  script é executado quando o arquivo é chamado diretamente
# chamando a função main().
if __name__ == "__main__":
    main()