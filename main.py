import json
import logging
import os
import re
import sys
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional
from urllib.parse import quote_plus

import gspread
import requests
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    ZoneInfo = None


logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("buscador-precos")


@dataclass
class Product:
    name: str
    price: float
    url: str


@dataclass
class AppConfig:
    search_keyword: str
    top_n: int
    google_sheet_id: str
    data_sheet_name: str
    target_sheet_name: str
    telegram_token: str
    telegram_chat_id: str
    telegram_enabled: bool


def load_config() -> AppConfig:
    search_keyword = os.getenv("SEARCH_KEYWORD", "Monitor 144hz").strip()
    top_n = int(os.getenv("TOP_N_RESULTS", "5"))

    google_sheet_id = os.getenv("GOOGLE_SHEET_ID", "").strip()
    telegram_token = os.getenv("TELEGRAM_TOKEN", "").strip()
    telegram_chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()

    telegram_enabled = bool(telegram_token and telegram_chat_id)

    if not google_sheet_id:
        raise ValueError("Defina a variável de ambiente GOOGLE_SHEET_ID.")

    return AppConfig(
        search_keyword=search_keyword,
        top_n=top_n,
        google_sheet_id=google_sheet_id,
        data_sheet_name=os.getenv("DATA_SHEET_NAME", "Historico").strip(),
        target_sheet_name=os.getenv("TARGET_SHEET_NAME", "PrecosAlvo").strip(),
        telegram_token=telegram_token,
        telegram_chat_id=telegram_chat_id,
        telegram_enabled=telegram_enabled,
    )


def parse_price_to_float(price_text: str) -> Optional[float]:
    if not price_text:
        return None

    cleaned = price_text.strip()
    cleaned = cleaned.replace("R$", "")
    cleaned = cleaned.replace("\u00a0", "")
    cleaned = re.sub(r"[^\d,\.]", "", cleaned)

    if not cleaned:
        return None

    if "," in cleaned:
        cleaned = cleaned.replace(".", "").replace(",", ".")

    try:
        return float(cleaned)
    except ValueError:
        return None


def brl(price: float) -> str:
    value = f"{price:,.2f}"
    return value.replace(",", "X").replace(".", ",").replace("X", ".")


def now_brt_str() -> str:
    if ZoneInfo:
        dt = datetime.now(ZoneInfo("America/Sao_Paulo"))
    else:
        dt = datetime.now()
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def normalize_url(url: str) -> str:
    return url.split("#")[0].split("?")[0].strip()


def build_search_url(keyword: str) -> str:
    return f"https://lista.mercadolivre.com.br/{quote_plus(keyword)}"


def scrape_top_product_links(keyword: str, limit: int) -> List[Dict[str, str]]:
    search_url = build_search_url(keyword)
    products: List[Dict[str, str]] = []
    seen_urls = set()

    logger.info("Abrindo página de busca: %s", search_url)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(search_url, wait_until="domcontentloaded", timeout=60000)
        page.wait_for_timeout(2500)

        cards = page.query_selector_all("li.ui-search-layout__item")

        for card in cards:
            link_el = card.query_selector("a.ui-search-link")
            title_el = card.query_selector("h3")

            if not link_el:
                continue

            url = link_el.get_attribute("href") or ""
            url = normalize_url(url)

            if not url or url in seen_urls:
                continue

            if not re.search(r"/MLB-", url, re.IGNORECASE):
                continue

            title = (title_el.inner_text().strip() if title_el else "Produto")

            products.append({"url": url, "title": title})
            seen_urls.add(url)

            if len(products) >= limit:
                break

        browser.close()

    logger.info("URLs coletadas: %d", len(products))
    return products


def scrape_product_detail(url: str, fallback_title: str) -> Optional[Product]:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        try:
            page.goto(url, wait_until="domcontentloaded", timeout=60000)
            page.wait_for_timeout(2000)
        except PlaywrightTimeoutError:
            logger.warning("Timeout ao abrir produto: %s", url)
            browser.close()
            return None

        title_selectors = ["h1.ui-pdp-title", "h1"]
        price_selectors = [
            "div.ui-pdp-price__second-line span.andes-money-amount__fraction",
            "span.andes-money-amount__fraction",
            "meta[itemprop='price']",
        ]

        title = fallback_title
        for selector in title_selectors:
            el = page.query_selector(selector)
            if el:
                text = (el.get_attribute("content") or el.inner_text() or "").strip()
                if text:
                    title = text
                    break

        fraction = None
        cents = None

        fraction_el = page.query_selector(
            "div.ui-pdp-price__second-line span.andes-money-amount__fraction"
        ) or page.query_selector("span.andes-money-amount__fraction")
        cents_el = page.query_selector(
            "div.ui-pdp-price__second-line span.andes-money-amount__cents"
        ) or page.query_selector("span.andes-money-amount__cents")

        if fraction_el:
            fraction = (fraction_el.inner_text() or "").strip()
        if cents_el:
            cents = (cents_el.inner_text() or "").strip()

        price_value = None

        if fraction:
            price_text = f"{fraction},{cents or '00'}"
            price_value = parse_price_to_float(price_text)

        if price_value is None:
            for selector in price_selectors:
                el = page.query_selector(selector)
                if not el:
                    continue

                raw = (el.get_attribute("content") or el.inner_text() or "").strip()
                price_value = parse_price_to_float(raw)
                if price_value is not None:
                    break

        browser.close()

        if price_value is None:
            logger.warning("Não foi possível extrair preço em: %s", url)
            return None

        return Product(name=title, price=price_value, url=url)


def scrape_mercadolivre(keyword: str, limit: int) -> List[Product]:
    links = scrape_top_product_links(keyword, limit)
    items: List[Product] = []

    for entry in links:
        product = scrape_product_detail(entry["url"], entry["title"])
        if product:
            items.append(product)

    logger.info("Produtos válidos extraídos: %d", len(items))
    return items


def get_gspread_client():
    credentials_json = os.getenv("GOOGLE_CREDENTIALS", "").strip()
    credentials_file = os.getenv("GOOGLE_CREDENTIALS_FILE", "").strip()

    if credentials_json:
        creds_dict = json.loads(credentials_json)
        return gspread.service_account_from_dict(creds_dict)

    if credentials_file:
        return gspread.service_account(filename=credentials_file)

    raise ValueError(
        "Defina GOOGLE_CREDENTIALS (JSON da service account) ou GOOGLE_CREDENTIALS_FILE (caminho)."
    )


def safe_float(value: str) -> Optional[float]:
    if value is None:
        return None

    if isinstance(value, (int, float)):
        return float(value)

    return parse_price_to_float(str(value))


def get_target_price(target_ws, product: Product) -> Optional[float]:
    if not target_ws:
        return None

    rows = target_ws.get_all_records()
    target_by_url = {}
    target_by_name = {}

    for row in rows:
        url = normalize_url(str(row.get("URL do Produto", "") or ""))
        name = str(row.get("Nome do Produto", "") or "").strip().lower()
        target = safe_float(row.get("Preço Alvo"))

        if target is None:
            continue

        if url:
            target_by_url[url] = target
        if name:
            target_by_name[name] = target

    norm_url = normalize_url(product.url)
    if norm_url in target_by_url:
        return target_by_url[norm_url]

    name_key = product.name.strip().lower()
    if name_key in target_by_name:
        return target_by_name[name_key]

    return None


def get_last_price(data_ws, product: Product) -> Optional[float]:
    rows = data_ws.get_all_records()
    norm_url = normalize_url(product.url)
    name_key = product.name.strip().lower()

    last_by_url = None
    last_by_name = None

    for row in reversed(rows):
        row_url = normalize_url(str(row.get("URL do Produto", "") or ""))
        row_name = str(row.get("Nome do Produto", "") or "").strip().lower()
        row_price = safe_float(row.get("Preço Encontrado"))

        if row_price is None:
            continue

        if last_by_url is None and row_url and row_url == norm_url:
            last_by_url = row_price

        if last_by_name is None and row_name and row_name == name_key:
            last_by_name = row_price

        if last_by_url is not None and last_by_name is not None:
            break

    return last_by_url if last_by_url is not None else last_by_name


def send_telegram_message(token: str, chat_id: str, message: str) -> None:
    if not token or not chat_id:
        logger.info("Telegram não configurado. Alerta ignorado.")
        return

    endpoint = f"https://api.telegram.org/bot{token}/sendMessage"
    response = requests.post(
        endpoint,
        json={"chat_id": chat_id, "text": message, "disable_web_page_preview": False},
        timeout=30,
    )

    if response.status_code >= 300:
        raise RuntimeError(f"Erro no Telegram: {response.status_code} - {response.text}")


def process_products(config: AppConfig, products: List[Product]) -> None:
    gc = get_gspread_client()
    sh = gc.open_by_key(config.google_sheet_id)

    data_ws = sh.worksheet(config.data_sheet_name)

    try:
        target_ws = sh.worksheet(config.target_sheet_name)
    except gspread.exceptions.WorksheetNotFound:
        logger.warning(
            "A aba '%s' não foi encontrada. Será usado somente histórico.",
            config.target_sheet_name,
        )
        target_ws = None

    timestamp = now_brt_str()

    rows_to_append = []
    for product in products:
        rows_to_append.append([timestamp, product.name, product.price, product.url])

        target_price = get_target_price(target_ws, product)
        last_price = get_last_price(data_ws, product)

        reference_price = target_price if target_price is not None else last_price

        if reference_price is not None and product.price < reference_price:
            message = (
                f"🚨 Preço caiu! {product.name} está custando R$ {brl(product.price)} "
                f"aqui: {product.url}"
            )
            if config.telegram_enabled:
                send_telegram_message(config.telegram_token, config.telegram_chat_id, message)
                logger.info("Alerta enviado para: %s", product.name)
            else:
                logger.info("Telegram desabilitado. Mensagem gerada: %s", message)

    if rows_to_append:
        data_ws.append_rows(rows_to_append, value_input_option="USER_ENTERED")
        logger.info("%d linhas inseridas na planilha.", len(rows_to_append))


def main() -> int:
    try:
        config = load_config()
        products = scrape_mercadolivre(config.search_keyword, config.top_n)

        if not products:
            logger.warning("Nenhum produto foi extraído.")
            return 0

        process_products(config, products)
        logger.info("Execução finalizada com sucesso.")
        return 0

    except Exception as exc:
        logger.exception("Falha na execução: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
