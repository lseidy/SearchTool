# SearchTool - Agregador de Precos Multi-Marketplace

Sistema de monitoramento de precos com:
- scraping em multiplos marketplaces (Mercado Livre, Amazon BR, Shopee, Magalu)
- persistencia e baseline no Google Sheets
- alertas visuais no Telegram (mensagem com foto)
- cadastro de itens via bot Telegram
- geracao automatica de blacklist (negative keywords) via Gemini
- automacao via GitHub Actions (hosted e self-hosted)

## O que o sistema faz hoje

1. Recebe termos de monitoramento via comando /add no Telegram.
2. Gera blacklist automatica por item usando Gemini 2.5 Flash.
3. Salva o item na aba PrecosAlvo com Chat_ID, termo, marketplace global, data e blacklist.
4. Em cada execucao do scraper:
- carrega todos os termos dos usuarios da planilha
- recalibra baseline global quando necessario
- busca produtos nas lojas configuradas
- aplica filtros de qualidade de titulo, URL e preco
- aplica blacklist local por item (escopo isolado por linha)
- escolhe o melhor preco global
- atualiza historico no Google Sheets
- envia alerta Telegram quando o desconto minimo e atingido

## Arquitetura

- telegram.py:
   - listener de comandos Telegram (long polling)
   - fluxo de cadastro com /add
   - integracao com Gemini para negative keywords
   - escrita da aba PrecosAlvo

- main.py:
   - scraping e consolidacao multi-marketplace
   - calibragem de baseline
   - monitoramento global por item
   - filtro por blacklist local
   - escrita da aba Historico
   - envio de alerta Telegram

## Estrutura de pastas e arquivos

Arvore principal do projeto:

```text
SearchTool/
|- .github/
|  |- workflows/
|  |  |- scraper.yml
|  |  |- scraper-selfhosted.yml
|- actions-runner/                # runner self-hosted local (diagnostico e execucao)
|- main.py                        # scraper e monitoramento
|- telegram.py                    # bot listener (/add, help, etc.)
|- requirements.txt               # dependencias Python
|- .env                           # variaveis locais (nao versionado)
|- .gitignore
|- README.md
```

Arquivos relevantes:
- .github/workflows/scraper.yml: pipeline em runner GitHub hospedado
- .github/workflows/scraper-selfhosted.yml: pipeline em runner self-hosted
- main.py: fluxo principal de monitoramento e alerta
- telegram.py: cadastro de termos e blacklist via Gemini

## Esquema das planilhas Google Sheets

### Aba Historico
Cabecalho esperado (autoajustado pelo sistema):
- Data/Hora
- Termo Buscado
- Preco Atual
- Preco Medio
- Menor Preco Historico
- Variacao (%)
- Link do Menor Preco Atual

### Aba PrecosAlvo
Cabecalho esperado (autoajustado pelo sistema):
- Chat_ID
- Termo Buscado
- Marketplace
- Preco Minimo
- Preco Maximo
- Data Ultima Calibragem
- Blacklist

## Variaveis de ambiente

### Gerais (main.py)
- SEARCH_KEYWORDS: lista de termos separados por | , ; ou quebra de linha
- SEARCH_KEYWORD: fallback quando SEARCH_KEYWORDS nao vier
- TOP_N_RESULTS: limite padrao de resultados
- CALIBRATION_TOP_N: volume de itens para calibragem
- MONITOR_TOP_N: quantidade alvo por loja no monitoramento
- MIN_PRICE_THRESHOLD: preco minimo aceito
- GOOGLE_SHEET_ID: ID da planilha
- GOOGLE_CREDENTIALS: JSON da service account em linha unica
- GOOGLE_CREDENTIALS_FILE: caminho para arquivo da credencial
- DATA_SHEET_NAME: nome da aba de historico (padrao Historico)
- TARGET_SHEET_NAME: nome da aba de metas (padrao PrecosAlvo)
- TELEGRAM_TOKEN: token do bot
- TELEGRAM_CHAT_ID: chat padrao de fallback
- LOG_LEVEL: nivel de logs
- PLAYWRIGHT_HEADLESS: true/false
- SCRAPER_PROXY_SERVER: proxy
- SCRAPER_PROXY_USERNAME: usuario do proxy
- SCRAPER_PROXY_PASSWORD: senha do proxy
- ML_ITEM_CONDITION: filtro de condicao no Mercado Livre
- ML_SHIPPING_ORIGIN: filtro de origem no Mercado Livre
- ML_NO_INDEX: habilita sufixo NoIndex no Mercado Livre

### Gemini (telegram.py)
- GEMINI_API_KEY: chave da API Gemini
- GOOGLE_API_KEY: fallback para GEMINI_API_KEY

## Como executar localmente

1. Instale dependencias:

```bash
pip install -r requirements.txt
```

2. Instale navegador do Playwright:

```bash
playwright install chromium
```

3. Execute o scraper:

```bash
python main.py
```

4. Execute o listener do Telegram (em outro terminal):

```bash
python telegram.py
```

## Fluxo do comando /add

Quando o usuario envia /add Produto X:

1. telegram.py recebe o comando.
2. Faz chamada ao Gemini 2.5 Flash para gerar ate 20 negative keywords.
3. Normaliza e deduplica os termos.
4. Insere linha em PrecosAlvo com blacklist preenchida.
5. O scraper usa essa blacklist apenas para o item daquela linha durante o loop.

## Regra critica de escopo (Blacklist local)

A blacklist nao e global.

Em main.py, para cada target carregado do Sheets:
- converte a string Blacklist da linha atual para lista Python
- injeta essa lista no monitoramento daquele item
- descarta itens cujo titulo contenha qualquer termo da blacklist local
- ao ir para o proximo item, a blacklist anterior nao e reutilizada

## Alertas Telegram

O sistema envia alerta quando encontra desconto relevante (>= 10% vs referencia/mediana):
- preferencialmente com sendPhoto (imagem + legenda rica)
- fallback para sendMessage quando nao houver imagem

Campos exibidos no alerta:
- produto
- preco atual
- preco medio/mediana
- percentual de desconto
- link do anuncio

## Agendamento automatico (GitHub Actions)

Workflows:
- .github/workflows/scraper.yml
- .github/workflows/scraper-selfhosted.yml

Triggers:
- schedule (cron em UTC)
- workflow_dispatch (manual)

Horarios atuais equivalentes em BRT:
- 06:00
- 07:30
- 09:00
- 10:30
- 12:00
- 13:30
- 15:00
- 16:30
- 18:00
- 19:30
- 21:00
- 22:30
- 00:00
- 00:30

## Dependencias

Arquivo: requirements.txt

- playwright==1.53.0
- gspread==6.1.2
- requests==2.32.4
- python-dotenv==1.0.1
- pandas==2.2.3

## Observacoes operacionais

- A pasta actions-runner contem runtime e logs do runner self-hosted.
- A pasta __pycache__ e gerada automaticamente pelo Python.
- .env e credenciais nao devem ser versionados.

## Checklist rapido de setup

1. Criar bot no Telegram e obter TELEGRAM_TOKEN.
2. Obter chat id e configurar TELEGRAM_CHAT_ID.
3. Criar service account Google, habilitar Sheets API e compartilhar planilha.
4. Definir GOOGLE_SHEET_ID e credenciais no ambiente.
5. Definir GEMINI_API_KEY para habilitar blacklist automatica no /add.
6. Rodar python telegram.py e python main.py.

## Roadmap tecnico sugerido

- Implementar comandos Telegram ainda pendentes (list, remove, status, trend, target, pause, resume).
- Adicionar testes automatizados para parse de preco e filtros de titulo.
- Criar arquivo .env.example sem segredos para onboarding mais rapido.
