import json
import logging
import os
import re
import sys
import unicodedata
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional
from urllib.parse import quote_plus

import gspread
import pandas as pd
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


DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

TITLE_BLACKLIST_EXACT = {
    "loja oficial",
    "ofertas",
    "mercado livre",
}

SEARCH_STOPWORDS = {
    "de",
    "do",
    "da",
    "dos",
    "das",
    "e",
    "com",
    "para",
    "a",
    "o",
}


def get_proxy_settings() -> Optional[Dict[str, str]]:
    server = os.getenv("SCRAPER_PROXY_SERVER", "").strip()
    username = os.getenv("SCRAPER_PROXY_USERNAME", "").strip()
    password = os.getenv("SCRAPER_PROXY_PASSWORD", "").strip()

    if not server:
        return None

    settings: Dict[str, str] = {"server": server}
    if username:
        settings["username"] = username
    if password:
        settings["password"] = password
    return settings


def get_requests_session() -> requests.Session:
    session = requests.Session()
    proxy_server = os.getenv("SCRAPER_PROXY_SERVER", "").strip()
    proxy_user = os.getenv("SCRAPER_PROXY_USERNAME", "").strip()
    proxy_pass = os.getenv("SCRAPER_PROXY_PASSWORD", "").strip()

    if proxy_server:
        if proxy_user and proxy_pass and "@" not in proxy_server:
            parts = proxy_server.split("://", 1)
            if len(parts) == 2:
                scheme, host = parts
                proxy_url = f"{scheme}://{proxy_user}:{proxy_pass}@{host}"
            else:
                proxy_url = proxy_server
        else:
            proxy_url = proxy_server

        session.proxies.update({"http": proxy_url, "https": proxy_url})
        logger.info("Proxy habilitado para requests.")

    return session


@dataclass
class Product:
    name: str
    price: float
    url: str


@dataclass
class AppConfig:
    search_keyword: str
    search_keywords: List[str]
    top_n: int
    google_sheet_id: str
    data_sheet_name: str
    target_sheet_name: str
    telegram_token: str
    telegram_chat_id: str
    telegram_enabled: bool


def parse_search_keywords() -> List[str]:
    raw_keywords = os.getenv("SEARCH_KEYWORDS", "").strip()
    fallback = os.getenv("SEARCH_KEYWORD", "Monitor 144hz").strip()

    if not raw_keywords:
        return [fallback]

    parsed = [
        part.strip(" \t\r\n|,;/-")
        for part in re.split(r"[,;|\n]+", raw_keywords)
        if part.strip()
    ]
    parsed = [term for term in parsed if term and term not in {"/", "-", "_"}]

    return parsed if parsed else [fallback]


def load_config() -> AppConfig:
    search_keywords = parse_search_keywords()
    search_keyword = search_keywords[0]
    top_n = int(os.getenv("TOP_N_RESULTS", "5"))

    google_sheet_id = os.getenv("GOOGLE_SHEET_ID", "").strip()
    telegram_token = os.getenv("TELEGRAM_TOKEN", "").strip()
    telegram_chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()

    telegram_enabled = bool(telegram_token and telegram_chat_id)

    if not google_sheet_id:
        raise ValueError("Defina a variável de ambiente GOOGLE_SHEET_ID.")

    return AppConfig(
        search_keyword=search_keyword,
        search_keywords=search_keywords,
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
    return f"https://lista.mercadolivre.com.br/{quote_plus(keyword)}_OrderId_PRICE"


def normalize_title_for_match(title: str) -> str:
    normalized = re.sub(r"[^\w\s]", " ", (title or "").strip().lower())
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized


def is_valid_product_url(url: str) -> bool:
    return bool(url and re.search(r"/(MLB-|p/MLB)", url, re.IGNORECASE))


def is_blacklisted_title(title: str) -> bool:
    return normalize_title_for_match(title) in TITLE_BLACKLIST_EXACT


def sanitize_products(products: List[Product]) -> List[Product]:
    sanitized: List[Product] = []
    seen_urls = set()

    for product in products:
        url = normalize_url(product.url)
        price = safe_float(product.price)
        title = (product.name or "").strip()

        if not is_valid_product_url(url):
            continue
        if price is None or price <= 0.0:
            continue
        if not title or is_blacklisted_title(title):
            continue
        if url in seen_urls:
            continue

        seen_urls.add(url)
        sanitized.append(Product(name=title, price=price, url=url))

    return sanitized


def normalize_text(text: str) -> str:
    ascii_text = unicodedata.normalize("NFKD", text or "").encode("ascii", "ignore").decode("ascii")
    normalized = re.sub(r"[^a-zA-Z0-9\s]", " ", ascii_text.lower())
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized


def tokenize_search_term(search_keyword: str) -> List[str]:
    tokens = normalize_text(search_keyword).split()
    return [t for t in tokens if t not in SEARCH_STOPWORDS and len(t) >= 2]


def relevance_score(title: str, keyword_tokens: List[str]) -> int:
    title_tokens = set(normalize_text(title).split())
    return sum(1 for token in keyword_tokens if token in title_tokens)


def filter_relevant_products(products: List[Product], search_keyword: str) -> List[Product]:
    keyword_tokens = tokenize_search_term(search_keyword)
    if not keyword_tokens:
        return products

    scored = [
        (product, relevance_score(product.name, keyword_tokens))
        for product in products
    ]

    high_relevance = [product for product, score in scored if score >= 2]
    if high_relevance:
        return high_relevance

    medium_relevance = [product for product, score in scored if score >= 1]
    if medium_relevance:
        return medium_relevance

    return products


def scrape_mercadolivre_api(keyword: str, limit: int) -> List[Product]:
    endpoint = "https://api.mercadolibre.com/sites/MLB/search"
    logger.info("Tentando fallback pela API pública do Mercado Livre.")

    session = get_requests_session()

    try:
        response = session.get(
            endpoint,
            params={"q": keyword, "limit": limit, "sort": "price_asc"},
            timeout=30,
            headers={"User-Agent": os.getenv("BROWSER_USER_AGENT", DEFAULT_USER_AGENT)},
        )
    except requests.RequestException as exc:
        logger.warning("Falha de rede na API do Mercado Livre: %s", exc)
        return []

    if response.status_code >= 300:
        logger.warning(
            "API do Mercado Livre respondeu %s. Fallback API ignorado.",
            response.status_code,
        )
        return []

    payload = response.json()
    results = payload.get("results", []) if isinstance(payload, dict) else []

    products: List[Product] = []
    for item in results:
        if not isinstance(item, dict):
            continue

        title = str(item.get("title", "") or "").strip()
        permalink = normalize_url(str(item.get("permalink", "") or ""))
        price_raw = item.get("price")

        if not title or not permalink:
            continue

        price = safe_float(price_raw)
        if price is None:
            continue

        products.append(Product(name=title, price=price, url=permalink))

        if len(products) >= limit:
            break

    products = sanitize_products(products)
    logger.info("Produtos coletados via API (sanitizados): %d", len(products))
    return products


def scrape_mercadolivre_http(keyword: str, limit: int) -> List[Product]:
    logger.info("Tentando fallback por HTML via requests.")
    search_url = build_search_url(keyword)

    headers = {
        "User-Agent": os.getenv("BROWSER_USER_AGENT", DEFAULT_USER_AGENT),
        "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
    }

    session = get_requests_session()

    try:
        response = session.get(search_url, headers=headers, timeout=30)
    except requests.RequestException as exc:
        logger.warning("Falha ao buscar HTML da listagem: %s", exc)
        return []

    if response.status_code >= 300:
        logger.warning("Listagem HTML respondeu %s.", response.status_code)
        return []

    html = response.text
    links = []
    seen = set()

    # padrão principal atual do Mercado Livre (poly-component__title)
    for match in re.finditer(
        r'<a[^>]*class=["\'][^"\']*poly-component__title[^"\']*["\'][^>]*href=["\']([^"\']+)["\']',
        html,
        flags=re.IGNORECASE,
    ):
        raw_url = match.group(1).replace("&amp;", "&")
        url = normalize_url(raw_url)
        if not url:
            continue
        if url.startswith("/"):
            url = f"https://www.mercadolivre.com.br{url}"
        if url in seen:
            continue
        seen.add(url)
        links.append(url)
        if len(links) >= limit:
            break

    # fallback genérico por links contendo MLB
        for match in re.finditer(r'href=["\']([^"\']*?/(?:MLB-|p/MLB)[^"\']+)["\']', html, flags=re.IGNORECASE):
        url = normalize_url(match.group(1))
        if not url:
            continue
        if url.startswith("/"):
            url = f"https://www.mercadolivre.com.br{url}"
        if url in seen:
            continue
        seen.add(url)
        links.append(url)
        if len(links) >= limit:
            break

    if not links:
        logger.warning("Fallback HTML não encontrou links de produtos.")
        return []

    products: List[Product] = []
    for url in links:
        try:
            detail = session.get(url, headers=headers, timeout=30)
        except requests.RequestException:
            continue

        if detail.status_code >= 300:
            continue

        page_html = detail.text
        title = None
        price = None

        ldjson_products = extract_products_from_ldjson(page_html, limit=1)
        if ldjson_products:
            title = ldjson_products[0].get("title")
            price = parse_price_to_float(ldjson_products[0].get("price_text") or "")

        if not title:
            title_match = re.search(r"<h1[^>]*>(.*?)</h1>", page_html, flags=re.IGNORECASE | re.DOTALL)
            if title_match:
                title = re.sub(r"<[^>]+>", "", title_match.group(1)).strip()

        if price is None:
            meta_price_match = re.search(
                r'<meta[^>]+itemprop=["\']price["\'][^>]+content=["\']([^"\']+)["\']',
                page_html,
                flags=re.IGNORECASE,
            )
            if meta_price_match:
                price = parse_price_to_float(meta_price_match.group(1))

        if title and price is not None:
            products.append(Product(name=title, price=price, url=url))

        if len(products) >= limit:
            break

    products = sanitize_products(products)
    logger.info("Produtos coletados via HTML requests (sanitizados): %d", len(products))
    return products


def extract_products_from_ldjson(html: str, limit: int) -> List[Dict[str, str]]:
    products: List[Dict[str, str]] = []
    seen = set()

    scripts = re.findall(
        r"<script[^>]*type=[\"']application/ld\+json[\"'][^>]*>(.*?)</script>",
        html,
        flags=re.IGNORECASE | re.DOTALL,
    )

    for raw in scripts:
        raw = raw.strip()
        if not raw:
            continue

        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            continue

        blocks = data if isinstance(data, list) else [data]
        for block in blocks:
            if not isinstance(block, dict):
                continue

            items = block.get("itemListElement")
            if not isinstance(items, list):
                continue

            for entry in items:
                item = entry.get("item", {}) if isinstance(entry, dict) else {}
                if not isinstance(item, dict):
                    continue

                url = normalize_url(str(item.get("url", "") or ""))
                name = str(item.get("name", "") or "").strip() or "Produto"
                price = None

                offers = item.get("offers")
                if isinstance(offers, dict):
                    price = str(offers.get("price", "") or "").strip()

                if not url or url in seen or not re.search(r"/MLB-", url, re.IGNORECASE):
                    continue

                products.append({"url": url, "title": name, "price_text": price})
                seen.add(url)

                if len(products) >= limit:
                    return products

    return products


def build_browser_context(browser):
    return browser.new_context(
        locale="pt-BR",
        timezone_id="America/Sao_Paulo",
        user_agent=os.getenv("BROWSER_USER_AGENT", DEFAULT_USER_AGENT),
        viewport={"width": 1366, "height": 768},
    )


def scrape_top_product_links(keyword: str, limit: int) -> List[Dict[str, str]]:
    search_url = build_search_url(keyword)
    products: List[Dict[str, str]] = []
    seen_urls = set()

    logger.info("Abrindo página de busca: %s", search_url)

    with sync_playwright() as p:
        headless = os.getenv("PLAYWRIGHT_HEADLESS", "true").lower() != "false"
        launch_args = {
            "headless": headless,
            "args": ["--disable-blink-features=AutomationControlled", "--no-sandbox"],
        }
        proxy_settings = get_proxy_settings()
        if proxy_settings:
            launch_args["proxy"] = proxy_settings
            logger.info("Proxy habilitado para Playwright.")

        browser = p.chromium.launch(
            **launch_args,
        )
        context = build_browser_context(browser)
        page = context.new_page()
        page.goto(search_url, wait_until="domcontentloaded", timeout=60000)
        try:
            page.wait_for_selector(
                "a.poly-component__title, a.ui-search-link, li.ui-search-layout__item, div.poly-card__content",
                timeout=15000,
            )
        except PlaywrightTimeoutError:
            logger.warning("Timeout aguardando resultados da busca.")
        page.wait_for_timeout(2500)
        logger.info("Título da página de busca: %s", page.title())

        html = page.content()
        lower_html = html.lower()
        bot_markers = [
            "pardon our interruption",
            "unusual traffic",
            "verify you are human",
            "captcha",
            "acesso negado",
            "cloudflare",
        ]
        if any(marker in lower_html for marker in bot_markers):
            logger.warning("Possível bloqueio anti-bot detectado na página de busca.")

        ldjson_products = extract_products_from_ldjson(html, limit)
        if ldjson_products:
            logger.info("URLs coletadas via JSON-LD: %d", len(ldjson_products))
            context.close()
            browser.close()
            return ldjson_products

        poly_links = page.query_selector_all(
            "a.poly-component__title, a.poly-card__title, a.ui-search-link, h3 a"
        )
        for link_el in poly_links:
            url = normalize_url(link_el.get_attribute("href") or "")
            if url.startswith("/"):
                url = f"https://www.mercadolivre.com.br{url}"
            if not url or url in seen_urls:
                continue
                if not is_valid_product_url(url):
                continue
            title = (link_el.inner_text() or "Produto").strip()
            products.append({"url": url, "title": title, "price_text": None})
            seen_urls.add(url)
            if len(products) >= limit:
                break

        if products:
            context.close()
            browser.close()
            logger.info("URLs coletadas (poly): %d", len(products))
            return products

        cards = page.query_selector_all("li.ui-search-layout__item")

        if not cards:
            logger.warning("Nenhum card encontrado com seletor principal. Tentando fallback por links.")
            fallback_links = page.eval_on_selector_all(
                "a[href*='/MLB-'], a[href*='/p/MLB']",
                "els => els.map(e => ({ href: e.href || '', text: (e.textContent || '').trim() }))",
            )

            for link in fallback_links:
                url = normalize_url(str(link.get("href", "") or ""))
                if not url or url in seen_urls:
                    continue
                    if not is_valid_product_url(url):
                    continue

                title = (str(link.get("text", "") or "").strip() or "Produto")
                products.append({"url": url, "title": title, "price_text": None})
                seen_urls.add(url)
                if len(products) >= limit:
                    break

            context.close()
            browser.close()
            logger.info("URLs coletadas (fallback): %d", len(products))
            return products

        for card in cards:
            link_el = card.query_selector("a.ui-search-link") or card.query_selector("a.poly-component__title")
            title_el = card.query_selector("h3") or card.query_selector("a.poly-component__title")
            fraction_el = card.query_selector("span.andes-money-amount__fraction")
            cents_el = card.query_selector("span.andes-money-amount__cents")

            if not link_el:
                continue

            url = link_el.get_attribute("href") or ""
            url = normalize_url(url)

            if not url or url in seen_urls:
                continue

                if not is_valid_product_url(url):
                continue

            title = (title_el.inner_text().strip() if title_el else "Produto")

            price_text = None
            if fraction_el:
                fraction = (fraction_el.inner_text() or "").strip()
                cents = (cents_el.inner_text() or "00").strip() if cents_el else "00"
                if fraction:
                    price_text = f"{fraction},{cents or '00'}"

            products.append({"url": url, "title": title, "price_text": price_text})
            seen_urls.add(url)

            if len(products) >= limit:
                break

        context.close()
        browser.close()

    logger.info("URLs coletadas: %d", len(products))
    return products


def scrape_product_detail(url: str, fallback_title: str) -> Optional[Product]:
    with sync_playwright() as p:
        headless = os.getenv("PLAYWRIGHT_HEADLESS", "true").lower() != "false"
        launch_args = {
            "headless": headless,
            "args": ["--disable-blink-features=AutomationControlled", "--no-sandbox"],
        }
        proxy_settings = get_proxy_settings()
        if proxy_settings:
            launch_args["proxy"] = proxy_settings

        browser = p.chromium.launch(**launch_args)
        context = build_browser_context(browser)
        page = context.new_page()

        try:
            page.goto(url, wait_until="domcontentloaded", timeout=60000)
            page.wait_for_timeout(2000)
        except PlaywrightTimeoutError:
            logger.warning("Timeout ao abrir produto: %s", url)
            context.close()
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

        context.close()
        browser.close()

        if price_value is None:
            logger.warning("Não foi possível extrair preço em: %s", url)
            return None

        return Product(name=title, price=price_value, url=url)


def scrape_mercadolivre(keyword: str, limit: int) -> List[Product]:
    links = scrape_top_product_links(keyword, limit)

    if not links:
        logger.warning("Sem links pela interface web. Usando fallback API.")
        api_items = scrape_mercadolivre_api(keyword, limit)
        if api_items:
            return sanitize_products(api_items)
        logger.warning("Fallback API sem resultados. Tentando fallback HTML requests.")
        return sanitize_products(scrape_mercadolivre_http(keyword, limit))

    items: List[Product] = []

    for entry in links:
        product = scrape_product_detail(entry["url"], entry["title"])
        if product:
            items.append(product)
            continue

        # fallback: tenta extrair preço da página de busca quando o detalhe falha
        logger.warning("Fallback para preço via busca: %s", entry["url"])
        fallback_price = parse_price_to_float(entry.get("price_text") or "")
        if fallback_price is not None:
            items.append(
                Product(name=entry["title"], price=fallback_price, url=entry["url"])
            )

    if not items:
        logger.warning("Sem produtos válidos pelo Playwright. Usando fallback API.")
        api_items = scrape_mercadolivre_api(keyword, limit)
        if api_items:
            return sanitize_products(api_items)
        logger.warning("Fallback API sem resultados. Tentando fallback HTML requests.")
        return sanitize_products(scrape_mercadolivre_http(keyword, limit))

    items = sanitize_products(items)
    logger.info("Produtos válidos extraídos (sanitizados): %d", len(items))
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


def log_google_sheets_preflight(gc, sheet_id: str) -> None:
    try:
        creds = getattr(gc, "auth", None)
        if creds is None:
            logger.error("Diagnóstico Google Sheets: credenciais ausentes no cliente gspread.")
            return

        from google.auth.transport.requests import Request as GoogleAuthRequest

        creds.refresh(GoogleAuthRequest())
        token = getattr(creds, "token", None)
        if not token:
            logger.error("Diagnóstico Google Sheets: token OAuth não foi gerado.")
            return

        endpoint = f"https://sheets.googleapis.com/v4/spreadsheets/{sheet_id}?includeGridData=false"
        response = requests.get(
            endpoint,
            headers={"Authorization": f"Bearer {token}"},
            timeout=30,
        )

        logger.error(
            "Diagnóstico Google Sheets preflight | status=%s | content-type=%s | body-preview=%s",
            response.status_code,
            response.headers.get("Content-Type", ""),
            (response.text or "")[:400],
        )
    except Exception as diag_exc:
        logger.error("Falha no diagnóstico Google Sheets preflight: %s", diag_exc)


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


def process_products(config: AppConfig, products: List[Product], search_keyword: str) -> None:
    gc = get_gspread_client()
    try:
        sh = gc.open_by_key(config.google_sheet_id)
    except gspread.exceptions.SpreadsheetNotFound as exc:
        raise RuntimeError(
            "Planilha não encontrada ou sem permissão. Verifique GOOGLE_SHEET_ID e compartilhe a planilha com o e-mail da service account."
        ) from exc
    except gspread.exceptions.APIError as exc:
        response = getattr(exc, "response", None)
        status_code = getattr(response, "status_code", None)
        content_type = response.headers.get("Content-Type", "") if response is not None else ""
        body_preview = (response.text or "")[:400] if response is not None else ""

        logger.error(
            "Falha Google Sheets API | status=%s | content-type=%s | body-preview=%s",
            status_code,
            content_type,
            body_preview,
        )
        log_google_sheets_preflight(gc, config.google_sheet_id)
        raise RuntimeError(
            "Falha ao acessar Google Sheets. Verifique GOOGLE_CREDENTIALS, GOOGLE_SHEET_ID, APIs do Google habilitadas e permissões da service account."
        ) from exc
    except requests.exceptions.JSONDecodeError as exc:
        logger.error(
            "Google Sheets retornou resposta não-JSON. Possível bloqueio de rede/proxy, credencial inválida ou endpoint inesperado."
        )
        log_google_sheets_preflight(gc, config.google_sheet_id)
        raise RuntimeError(
            "Resposta inválida da Google Sheets API. Verifique rede/proxy do runner, GOOGLE_CREDENTIALS e permissões da planilha."
        ) from exc
    except Exception as exc:
        logger.error("Erro inesperado ao abrir planilha: %s", exc)
        log_google_sheets_preflight(gc, config.google_sheet_id)
        raise

    data_ws = sh.worksheet(config.data_sheet_name)

    expected_headers = [
        "Data/Hora",
        "Termo Buscado",
        "Preço Atual",
        "Preço Médio",
        "Menor Preço Histórico",
        "Variação (%)",
        "Link do Menor Preço Atual",
    ]

    current_headers = data_ws.row_values(1)
    if current_headers != expected_headers:
        data_ws.update("A1:G1", [expected_headers], value_input_option="USER_ENTERED")

    valid_products = sanitize_products(products)
    valid_products = filter_relevant_products(valid_products, search_keyword)
    if not valid_products:
        logger.warning("Nenhum produto válido após sanitização.")
        return

    df = pd.DataFrame(
        [{"name": p.name, "price": p.price, "url": p.url} for p in valid_products]
    )

    if df.empty:
        logger.warning("DataFrame vazio após processamento.")
        return

    min_idx = df["price"].idxmin()
    current_price = float(df.loc[min_idx, "price"])
    average_price = float(df["price"].mean())
    current_link = str(df.loc[min_idx, "url"])
    timestamp = now_brt_str()

    rows = data_ws.get_all_records()
    keyword_key = search_keyword.strip().lower()

    existing_row_index = None
    existing_row = None
    for idx, row in enumerate(rows, start=2):
        term = str(row.get("Termo Buscado", "") or "").strip().lower()
        if term == keyword_key:
            existing_row_index = idx
            existing_row = row
            break

    variation_pct = 0.0
    historical_min = current_price
    previous_historical_min = None

    if existing_row is not None:
        previous_historical_min = safe_float(existing_row.get("Menor Preço Histórico"))
        if previous_historical_min is not None and previous_historical_min > 0:
            variation_pct = ((current_price - previous_historical_min) / previous_historical_min) * 100
            historical_min = min(current_price, previous_historical_min)
        else:
            variation_pct = 0.0
            historical_min = current_price

    payload = [
        timestamp,
        search_keyword,
        current_price,
        average_price,
        historical_min,
        variation_pct,
        current_link,
    ]

    if existing_row_index is None:
        data_ws.append_row(payload, value_input_option="USER_ENTERED")
        logger.info("Novo termo inserido no histórico: %s", search_keyword)
    else:
        data_ws.update(
            f"A{existing_row_index}:G{existing_row_index}",
            [payload],
            value_input_option="USER_ENTERED",
        )
        logger.info("Termo atualizado no histórico: %s", search_keyword)

    if existing_row is not None and variation_pct < 0:
        old_reference = previous_historical_min if previous_historical_min is not None else historical_min
        message = (
            f"📉 Novo recorde de preço para '{search_keyword}'!\n"
            f"Preço atual: R$ {brl(current_price)}\n"
            f"Recorde anterior: R$ {brl(old_reference)}\n"
            f"Variação: {variation_pct:.2f}%\n"
            f"Link: {current_link}"
        )
        if config.telegram_enabled:
            send_telegram_message(config.telegram_token, config.telegram_chat_id, message)
            logger.info("Alerta de novo menor preço enviado.")
        else:
            logger.info("Telegram desabilitado. Mensagem gerada: %s", message)


def main() -> int:
    try:
        config = load_config()
        processed_any_term = False

        for keyword in config.search_keywords:
            logger.info("Iniciando busca para termo: %s", keyword)
            products = scrape_mercadolivre(keyword, config.top_n)

            if not products:
                logger.warning("Nenhum produto foi extraído para o termo: %s", keyword)
                continue

            process_products(config, products, keyword)
            processed_any_term = True

        if not processed_any_term:
            logger.warning("Nenhum termo retornou produtos válidos.")
            return 0

        logger.info("Execução finalizada com sucesso.")
        return 0

    except Exception as exc:
        logger.exception("Falha na execução: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
