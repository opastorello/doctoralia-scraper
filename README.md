# Scraper para Doctoralia

Este projeto contém um script Python para extração de dados (web scraping) de perfis de médicos do site Doctoralia. O script é projetado para ser robusto, resiliente e flexível, oferecendo diferentes modos de operação para coletar ou atualizar informações.

## Funcionalidades Principais

- **Detecção Dinâmica de Páginas**: Identifica automaticamente o número total de páginas de resultados para uma busca, garantindo a coleta completa.
- **Dois Modos de Operação**:
  - `new`: Realiza uma nova busca para encontrar perfis de médicos que ainda não estão no banco de dados.
  - `update`: Re-raspa todos os perfis já existentes no banco de dados para atualizar suas informações.
- **Extração de Dados Detalhada**: Coleta as seguintes informações de cada perfil:
  - Nome completo
  - URL do perfil
  - Especialização principal
  - Múltiplos números de telefone
  - Múltiplos endereços de consultório
- **Resiliência a Bloqueios**: Implementa um sistema de repetição automática (`retry`) com pausas (`sleep`) para lidar com erros de conexão ou bloqueios temporários (ex: CAPTCHA, erro 405).
- **Processamento Paralelo**: Utiliza `ThreadPoolExecutor` para extrair os detalhes de múltiplos perfis simultaneamente, acelerando o processo.
- **Armazenamento Persistente**: Salva todos os dados coletados em um banco de dados SQLite local (`doctoralia_data.db`).

## Setup e Instalação

**Pré-requisitos**:
- Python 3.9 ou superior

**Passos**:

1. **Clone o repositório:**
   ```sh
   git clone https://github.com/opastorello/doctoralia-scraper.git
   cd doctoralia-scraper
   ```

2. **(Recomendado) Crie e ative um ambiente virtual:**
   ```sh
   # Para Linux/macOS
   python3 -m venv venv
   source venv/bin/activate

   # Para Windows
   python -m venv venv
   .\venv\Scripts\activate
   ```

3. **Instale as dependências:**
   ```sh
   pip install -r requirements.txt
   ```

## Como Usar

O script é executado através da linha de comando e aceita o argumento `--mode` para definir seu comportamento.

#### **Modo 1: Nova Raspagem (Padrão)**

Para buscar novos perfis que ainda não estão no banco de dados, execute:
```sh
python src/scraper.py --mode new
```
ou simplesmente:
```sh
python src/scraper.py
```

#### **Modo 2: Atualizar Perfis Existentes**

Para re-raspar e atualizar as informações de todos os médicos já salvos no seu banco de dados, execute:
```sh
python src/scraper.py --mode update
```

### Modificando a URL de Busca

Para alterar a busca inicial (ex: outra especialidade ou cidade), modifique a variável `initial_search_url` dentro da função `main()` no arquivo `src/scraper.py`.

### Dados de Saída

Os dados coletados são salvos no arquivo `doctoralia_data.db` na raiz do projeto. Você pode usar qualquer visualizador de SQLite para abrir e exportar os dados.

## Aviso

Este script foi desenvolvido para fins educacionais. Ao utilizá-lo, respeite os termos de serviço do site Doctoralia e as boas práticas de web scraping. Evite fazer um número excessivo de requisições em um curto período para não sobrecarregar o servidor do site.
