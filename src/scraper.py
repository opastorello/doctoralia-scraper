# -*- coding: utf-8 -*-
"""
Script para automação e web scraping de perfis de médicos no site Doctoralia.

Propósito:
- Possui dois modos de execução:
  1. 'new': Acessa URLs de pesquisa para encontrar e salvar novos perfis.
  2. 'update': Re-raspa todos os perfis já existentes no banco de dados
     para atualizar suas informações.
- Detecta dinamicamente o número total de páginas de resultados para garantir uma
  raspagem completa.
- Extrai múltiplos telefones, a especialização principal e o(s) endereço(s) de cada médico.
- Armazena os dados consolidados em um banco de dados SQLite local.
- Inclui um sistema de pausa e repetição (retry) para lidar com bloqueios.

Dependências:
- beautifulsoup4>=4.0.0
- requests>=2.25.0
"""

import sqlite3
import logging
import re
import time
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Set, Optional

import requests
from bs4 import BeautifulSoup

# --- Configuração Inicial ---

# Configuração de logging para monitoramento da execução
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(threadName)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

# Constantes
BASE_URL = "https://www.doctoralia.com.br"
DB_NAME = "doctoralia_data.db"
# --- Configuração do Sistema de Repetição (Retry) ---
MAX_RETRIES = 3
RETRY_DELAY_SECONDS = 10

# Cabeçalhos baseados na requisição cURL para simular um navegador real
HEADERS = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
    'priority': 'u=0, i',
    'sec-ch-ua': '"Not)A;Brand";v="8", "Chromium";v="138", "Google Chrome";v="138"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Linux"',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'same-origin',
    'sec-fetch-user': '?1',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36',
}

# --- Módulo de Banco de Dados ---

def setup_database(db_name: str) -> None:
    """Cria o banco de dados e a tabela se não existirem."""
    try:
        with sqlite3.connect(db_name) as conn:
            cursor = conn.cursor()
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS doctors (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                profile_url TEXT UNIQUE NOT NULL,
                phone TEXT,
                specialization TEXT,
                address TEXT,
                scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """)
            # Garante que as colunas existam em tabelas antigas
            for col in ['specialization', 'address']:
                try:
                    cursor.execute(f"ALTER TABLE doctors ADD COLUMN {col} TEXT;")
                except sqlite3.OperationalError:
                    pass  # Coluna já existe
            conn.commit()
            logger.info(f"Banco de dados '{db_name}' e tabela 'doctors' prontos.")
    except sqlite3.Error as e:
        logger.error(f"Erro ao configurar o banco de dados: {e}")
        raise

def get_existing_ids(db_name: str) -> Set[int]:
    """Busca todos os IDs de médicos já salvos no banco de dados."""
    try:
        with sqlite3.connect(db_name) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT id FROM doctors")
            return {row[0] for row in cursor.fetchall()}
    except sqlite3.Error as e:
        logger.error(f"Erro ao buscar IDs existentes no banco de dados: {e}")
        return set()

def get_all_profiles_from_db(db_name: str) -> List[Dict]:
    """Busca todos os perfis de médicos existentes no banco de dados."""
    profiles = []
    query = "SELECT id, name, profile_url FROM doctors"
    try:
        with sqlite3.connect(db_name) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute(query)
            for row in cursor.fetchall():
                profiles.append(dict(row))
        logger.info(f"Encontrados {len(profiles)} perfis no banco de dados para atualização.")
        return profiles
    except sqlite3.Error as e:
        logger.error(f"Erro ao buscar todos os perfis do banco de dados: {e}")
        return []

def save_to_database(db_name: str, doctors_data: List[Dict]) -> None:
    """Salva ou atualiza uma lista de dados de médicos no banco de dados."""
    if not doctors_data:
        return
    query = """
    INSERT OR REPLACE INTO doctors (id, name, profile_url, phone, specialization, address, scraped_at)
    VALUES (:id, :name, :profile_url, :phone, :specialization, :address, CURRENT_TIMESTAMP)
    """
    try:
        with sqlite3.connect(db_name) as conn:
            cursor = conn.cursor()
            for doc in doctors_data:
                doc.setdefault('phone', None)
                doc.setdefault('specialization', None)
                doc.setdefault('address', None)
            cursor.executemany(query, doctors_data)
            conn.commit()
            logger.info(f"Salvou/atualizou {len(doctors_data)} registros no banco de dados.")
    except sqlite3.Error as e:
        logger.error(f"Erro ao salvar dados no banco de dados: {e}")
        raise

# --- Módulo de Scraping ---

def get_total_pages(soup: BeautifulSoup) -> int:
    """Extrai o número total de páginas do bloco de paginação."""
    pagination_container = soup.find('aside', {'data-test-id': 'listing-pagination'})
    if not pagination_container:
        logger.info("Bloco de paginação não encontrado, assumindo 1 página.")
        return 1
    
    page_numbers = []
    page_links = pagination_container.find_all('a', class_='page-link')
    for link in page_links:
        page_text = link.get_text(strip=True)
        if page_text.isdigit():
            page_numbers.append(int(page_text))
    
    if not page_numbers:
        logger.info("Nenhum número de página encontrado, assumindo 1 página.")
        return 1
        
    total_pages = max(page_numbers)
    logger.info(f"Total de páginas detectado: {total_pages}")
    return total_pages

def scrape_search_results(session: requests.Session, initial_url: str, existing_ids: Set[int]) -> List[Dict]:
    """Varre as páginas de resultados de forma dinâmica."""
    doctors_to_process = []
    
    # Etapa 1: Acessar a primeira página para descobrir o total de páginas
    first_page_url = f"{initial_url.split('&page=')[0]}&page=1"
    logger.info("Acessando a primeira página para determinar o número total de páginas...")
    
    response = None
    for attempt in range(MAX_RETRIES + 1):
        try:
            if attempt > 0:
                delay = RETRY_DELAY_SECONDS * attempt
                logger.warning(f"Erro na página 1. Tentando novamente em {delay}s... (Tentativa {attempt}/{MAX_RETRIES})")
                time.sleep(delay)
            response = session.get(first_page_url, timeout=20)
            response.raise_for_status()
            break
        except requests.exceptions.RequestException as e:
            if attempt == MAX_RETRIES:
                logger.error(f"Não foi possível acessar a primeira página: {e}")
                return []

    first_page_soup = BeautifulSoup(response.text, 'html.parser')
    total_pages = get_total_pages(first_page_soup)

    # Etapa 2: Iterar por todas as páginas detectadas
    for page_num in range(1, total_pages + 1):
        soup = None
        if page_num == 1:
            logger.info(f"Processando a página de busca: 1/{total_pages}")
            soup = first_page_soup
        else:
            url = f"{initial_url.split('&page=')[0]}&page={page_num}"
            logger.info(f"Raspando a página de busca: {page_num}/{total_pages}")
            
            response = None
            for attempt in range(MAX_RETRIES + 1):
                try:
                    if attempt > 0:
                        delay = RETRY_DELAY_SECONDS * attempt
                        logger.warning(f"Erro na página {page_num}. Tentando novamente em {delay}s... (Tentativa {attempt}/{MAX_RETRIES})")
                        time.sleep(delay)
                    response = session.get(url, timeout=20)
                    response.raise_for_status()
                    break
                except requests.exceptions.RequestException as e:
                    if attempt == MAX_RETRIES:
                        logger.error(f"Falha definitiva ao raspar a página {page_num}: {e}")
                        response = None

            if response is None:
                logger.warning(f"Pulando a página {page_num} devido a falhas de rede.")
                continue
            
            soup = BeautifulSoup(response.text, 'html.parser')

        # Lógica comum de extração de dados da página
        calendar_blocks = soup.find_all('calendar-availability-app')
        for block in calendar_blocks:
            result_id = int(block.get('result-id'))
            if result_id in existing_ids:
                continue
            
            doctor_info = {
                "id": result_id,
                "name": block.get('result-name', 'N/A').strip(),
                "profile_url": block.get('url'),
            }
            if doctor_info['profile_url']:
                logger.info(f"Encontrado na busca: {doctor_info['name']} (ID: {doctor_info['id']})")
                doctors_to_process.append(doctor_info)
                existing_ids.add(result_id)
        
        if page_num < total_pages:
            time.sleep(1)
            
    return doctors_to_process

def fetch_profile_details(session: requests.Session, doctor_info: Dict) -> Dict:
    """Busca múltiplos telefones, especialização e endereço(s) em uma página de perfil."""
    doctor_id = doctor_info["id"]
    profile_url = doctor_info["profile_url"]
    
    phone, specialization, address = None, None, None
    
    response = None
    for attempt in range(MAX_RETRIES + 1):
        try:
            if attempt > 0:
                delay = RETRY_DELAY_SECONDS * attempt
                logger.warning(f"Erro no perfil ID {doctor_id}. Tentando novamente em {delay}s... (Tentativa {attempt}/{MAX_RETRIES})")
                time.sleep(delay)
            response = session.get(profile_url, timeout=15)
            response.raise_for_status()
            break
        except requests.exceptions.RequestException as e:
            if attempt == MAX_RETRIES:
                logger.error(f"Falha definitiva ao acessar perfil ID {doctor_id}: {e}")
                response = None
    
    if response:
        try:
            soup = BeautifulSoup(response.text, 'html.parser')
            page_content = response.text

            # Extrai a especialização
            spec_span = soup.find('span', {'data-test-id': 'doctor-specializations'})
            if spec_span and spec_span.find('a'):
                specialization = spec_span.find('a').get_text(strip=True)

            # Extrai todos os telefones encontrados
            phone_pattern = r'\(?\b\d{2}\b\)?\s?\d{4,5}-?\d{4}'
            found_phones = re.findall(phone_pattern, page_content)
            if found_phones:
                phone = ", ".join(sorted(list(set(found_phones))))
            
            # Extrai todos os endereços encontrados
            address_list = []
            address_blocks = soup.find_all('span', {'itemprop': 'streetAddress'})
            if address_blocks:
                for block in address_blocks:
                    address_parts = [part.strip() for part in block.stripped_strings]
                    full_address = " ".join(address_parts)
                    address_list.append(full_address)
            
            if address_list:
                address = "; ".join(address_list)

        except Exception as e:
            logger.error(f"Erro ao analisar o conteúdo do perfil ID {doctor_id}: {e}")

    doctor_info.update({"phone": phone, "specialization": specialization, "address": address})
    return doctor_info

# --- Orquestrador Principal ---

def main(args):
    """Função principal que orquestra o processo de scraping com base no modo selecionado."""
    initial_search_url = "https://www.doctoralia.com.br/pesquisa?q=&loc=S%C3%A3o%20Paulo&filters%5Bentity_type%5D%5B0%5D=doctor&filters%5Bdistricts%5D%5B0%5D=3631&filters%5Bdistricts%5D%5B1%5D=147&filters%5Bdistricts%5D%5B2%5D=51&filters%5Bdistricts%5D%5B3%5D=2813&filters%5Bdistricts%5D%5B4%5D=445&filters%5Bdistricts%5D%5B5%5D=353&filters%5Bdistricts%5D%5B6%5D=190&filters%5Bdistricts%5D%5B7%5D=615&filters%5Bdistricts%5D%5B8%5D=555&filters%5Bdistricts%5D%5B9%5D=127&filters%5Bdistricts%5D%5B10%5D=62&filters%5Bdistricts%5D%5B11%5D=7354&filters%5Bdistricts%5D%5B12%5D=79"
    
    setup_database(DB_NAME)
    profiles_to_process = []
    
    with requests.Session() as session:
        session.headers.update(HEADERS)
        
        logger.info("Iniciando sessão e obtendo cookies...")
        session.get(BASE_URL)
        time.sleep(1)

        if args.mode == 'new':
            logger.info("--- MODO: Nova Raspagem ---")
            existing_ids = get_existing_ids(DB_NAME)
            logger.info(f"Encontrados {len(existing_ids)} perfis já existentes, que serão ignorados na busca.")
            profiles_to_process = scrape_search_results(session, initial_search_url, existing_ids)
        
        elif args.mode == 'update':
            logger.info("--- MODO: Atualização de Perfis Existentes ---")
            profiles_to_process = get_all_profiles_from_db(DB_NAME)
    
        if not profiles_to_process:
            logger.info("Nenhum perfil para processar. Encerrando.")
            return

        logger.info(f"Total de {len(profiles_to_process)} perfis para extrair detalhes.")
        
        completed_doctors = []
        with ThreadPoolExecutor(max_workers=8, thread_name_prefix='ScraperThread') as executor:
            future_to_doctor = {executor.submit(fetch_profile_details, session, doc): doc for doc in profiles_to_process}
            
            for future in as_completed(future_to_doctor):
                try:
                    result = future.result()
                    completed_doctors.append(result)
                    logger.info(f"Processado: {result['name']} (ID: {result['id']}) - Especialização: {result.get('specialization')} - Telefones: {bool(result['phone'])} - Endereço: {bool(result['address'])}")
                except Exception as exc:
                    doctor_info = future_to_doctor[future]
                    logger.error(f"Erro ao processar o perfil do médico ID {doctor_info['id']}: {exc}")
    
    if completed_doctors:
        save_to_database(DB_NAME, completed_doctors)
    
    logger.info("--- Scraper Doctoralia Finalizado ---")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Scraper para Doctoralia com modos de execução.",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument(
        '--mode',
        type=str,
        choices=['new', 'update'],
        default='new',
        help=(
            "Modo de execução:\n"
            "'new'    - Busca por novos perfis nas páginas de pesquisa (padrão).\n"
            "'update' - Re-raspa todos os perfis já salvos no banco de dados."
        )
    )
    args = parser.parse_args()
    
    main(args)
