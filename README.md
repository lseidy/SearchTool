# Buscador de Preços (Mercado Livre + Google Sheets + Telegram)

## Visão Geral
Automação em Python para:
1. Buscar produtos no Mercado Livre por palavra-chave.
2. Extrair os 5 primeiros produtos.
3. Salvar histórico em Google Sheets.
4. Enviar alerta no Telegram quando o preço cair.
5. Rodar automaticamente a cada 1 hora via GitHub Actions.

## Estrutura esperada da planilha
Crie uma planilha no Google Sheets com as abas:

### 1) Aba `Historico`
Cabeçalho na linha 1:
- Data e Hora
- Nome do Produto
- Preço Encontrado
- URL do Produto

### 2) Aba `PrecosAlvo` (opcional, mas recomendada)
Cabeçalho na linha 1:
- Nome do Produto
- URL do Produto
- Preço Alvo

> Se a aba `PrecosAlvo` não existir, o sistema compara o preço atual com o último preço registrado no `Historico`.

## Variáveis de ambiente
- `SEARCH_KEYWORD` (ex.: `Monitor 144hz`)
- `TOP_N_RESULTS` (ex.: `5`)
- `GOOGLE_SHEET_ID`
- `GOOGLE_CREDENTIALS` (JSON completo da Service Account em uma única string)
- `TELEGRAM_TOKEN`
- `TELEGRAM_CHAT_ID`
- `DATA_SHEET_NAME` (default: `Historico`)
- `TARGET_SHEET_NAME` (default: `PrecosAlvo`)

## Setup local (opcional)
```bash
pip install -r requirements.txt
playwright install chromium
python main.py
```

## Passo a passo de credenciais

### 1) Google Cloud Console (Service Account)
1. Acesse o Google Cloud Console e crie (ou selecione) um projeto.
2. Ative a **Google Sheets API**.
3. Vá em **IAM e administrador > Contas de serviço**.
4. Crie uma conta de serviço.
5. Gere uma chave JSON da conta de serviço.
6. Copie o conteúdo do JSON para usar em `GOOGLE_CREDENTIALS`.
7. Compartilhe a planilha com o e-mail da conta de serviço (permissão de Editor).

### 2) Telegram Bot (BotFather)
1. No Telegram, abra o **@BotFather**.
2. Execute `/newbot` e siga os passos.
3. Guarde o token gerado (`TELEGRAM_TOKEN`).
4. Para obter o `TELEGRAM_CHAT_ID`, envie mensagem para o bot e consulte:
   - `https://api.telegram.org/bot<SEU_TOKEN>/getUpdates`
5. Copie o `chat.id` retornado.

### 3) GitHub Secrets
No repositório, abra **Settings > Secrets and variables > Actions** e crie:
- `GOOGLE_SHEET_ID`
- `GOOGLE_CREDENTIALS`
- `TELEGRAM_TOKEN`
- `TELEGRAM_CHAT_ID`

## Workflow GitHub Actions
Arquivo: `.github/workflows/scraper.yml`
- Executa a cada 1 hora (`cron: 0 * * * *`)
- Também pode ser executado manualmente (`workflow_dispatch`)
