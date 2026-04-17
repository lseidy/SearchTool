import json
import asyncio
from html import escape
import logging
import os
import re
import sys
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
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

DEFAULT_MARKETPLACE = "mercadolivre"


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

BLACKLIST_WORDS = [
    "capa",
    "case",
    "cartão",
    "pelicula",
    "película",
    "adesivo",
    "skin",
    "suporte",
    "cabo",
    "carregador",
    "fone",
    "carcaca",
    "carcaça",
    "peca",
    "reparo",
    "conserto",
    "manutencao",
    "manutenção",
    "alca",
    "alça",
    "bag",
    "bolsa",
]

PREPOSITION_GUARD_TERMS = [
    "para",
    "de",
    "compatível com",
    "compativel com",
]

QUANTITY_PATTERN = re.compile(r"(\d+(?:[\.,]\d+)?)\s*(kg|g|l|ml)\b", re.IGNORECASE)


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
    image_url: Optional[str] = None


@dataclass
class AppConfig:
    search_keyword: str
    search_keywords: List[str]
    top_n: int
    calibration_top_n: int
    monitor_top_n: int
    min_price_threshold: float
    google_sheet_id: str
    data_sheet_name: str
    target_sheet_name: str
    scraper_log_sheet_name: str
    telegram_token: str
    telegram_chat_id: str
    telegram_enabled: bool


class MultiMarketplaceScraper:
    SITE_CONFIG: Dict[str, Dict[str, Any]] = {
        "mercadolivre": {
            "base_url": "https://lista.mercadolivre.com.br/{query}",
            "sort_lowest": "_OrderId_PRICE",
            "range_mode": "path_reais",
            "selectors": {
                "cards": "li.ui-search-layout__item, div.poly-card",
                "title": "h3, a.poly-component__title, a.poly-card__title, a.ui-search-link",
                "price_whole": "span.andes-money-amount__fraction",
                "price_fraction": "span.andes-money-amount__cents",
                "link": "a.ui-search-link, a.poly-component__title, a.poly-card__title, h3 a",
                "image": "img.poly-component__picture, img.ui-search-result-image__element, img",
            },
        },
        "amazon": {
            "base_url": "https://www.amazon.com.br/s?k={query}",
            "sort_lowest": "s=price-asc-rank",
            "range_mode": "query_cents",
            "range_param": "rh=p_36:{min}-{max}",
            "range_param_min_only": "rh=p_36:{min}-",
            "range_param_max_only": "rh=p_36:-{max}",
            "selectors": {
                "cards": "div[data-component-type='s-search-result']",
                "title": "h2 span",
                "price_whole": "span.a-price-whole",
                "price_fraction": "span.a-price-fraction",
                "link": "h2 a.a-link-normal",
                "image": "img.s-image",
            },
        },
        "shopee": {
            "base_url": "https://shopee.com.br/search?keyword={query}",
            "sort_lowest": "sortBy=price&order=asc",
            "range_mode": "query_shopee_filter",
            "selectors": {
                "cards": "div.shopee-search-item-result__item, div[data-sqe='item']",
                "title": "div[data-sqe='name'], div.line-clamp-2",
                "price_whole": "span:has-text('R$'), div:has-text('R$')",
                "price_fraction": "",
                "link": "a[data-sqe='link'], a.contents",
                "image": "img",
            },
        },
        "magalu": {
            "base_url": "https://www.magazineluiza.com.br/busca/{query}/",
            "sort_lowest": "sortOrientation=asc&sortType=price",
            "range_mode": "query_magalu_filter",
            "selectors": {
                "cards": "[data-testid='product-card-content'], [data-testid='product-card-container']",
                "title": "[data-testid='product-title']",
                "price_whole": "[data-testid='price-value']",
                "price_fraction": "",
                "link": "[data-testid='product-card-container'], a",
                "image": "img",
            },
        },
    }

    def __init__(self, browser_context_builder):
        self.browser_context_builder = browser_context_builder

    def normalize_marketplace(self, marketplace: Optional[str]) -> str:
        key = (marketplace or DEFAULT_MARKETPLACE).strip().lower()
        if key not in self.SITE_CONFIG:
            return DEFAULT_MARKETPLACE
        return key

    def build_search_url(
        self,
        marketplace: str,
        keyword: str,
        price_min: Optional[float] = None,
        price_max: Optional[float] = None,
        sort_by_price: bool = True,
        start_offset: int = 1,
    ) -> str:
        site_key = self.normalize_marketplace(marketplace)
        site = self.SITE_CONFIG[site_key]
        query = quote_plus(keyword)
        url = site["base_url"].format(query=query)

        if site_key == "mercadolivre":
            offset_part = f"_Desde_{start_offset}" if start_offset > 1 else ""
            sort_part = site["sort_lowest"] if sort_by_price else ""
            range_part = ""
            if price_min is not None and price_max is not None:
                min_int = max(0, int(round(price_min)))
                max_int = max(min_int, int(round(price_max)))
                range_part = f"_PriceRange_{min_int}BRL-{max_int}BRL"
            return f"{url}{offset_part}{sort_part}{range_part}{build_marketplace_filters_suffix()}"

        query_params: List[str] = []
        if sort_by_price and site.get("sort_lowest"):
            query_params.append(site["sort_lowest"])

        if price_min is not None or price_max is not None:
            min_bound = max(0.0, float(price_min)) if price_min is not None else None
            max_bound = max(0.0, float(price_max)) if price_max is not None else None

            if min_bound is not None and max_bound is not None and max_bound < min_bound:
                max_bound = min_bound

            min_value = int(round(min_bound)) if min_bound is not None else 0
            max_value = int(round(max_bound)) if max_bound is not None else min_value

            if site.get("range_mode") == "query_cents":
                min_cents = int(round(min_bound * 100)) if min_bound is not None else None
                max_cents = int(round(max_bound * 100)) if max_bound is not None else None

                if min_cents is not None and max_cents is not None:
                    range_template = site.get("range_param", "")
                    range_param = range_template.format(min=min_cents, max=max_cents)
                elif min_cents is not None:
                    range_template = site.get("range_param_min_only", "rh=p_36:{min}-")
                    range_param = range_template.format(min=min_cents)
                elif max_cents is not None:
                    range_template = site.get("range_param_max_only", "rh=p_36:-{max}")
                    range_param = range_template.format(max=max_cents)
                else:
                    range_param = ""

                if range_param:
                    query_params.append(range_param)

            elif site.get("range_mode") == "query_shopee_filter":
                # Ex.: fe_filter_options=[{"group_name":"PRICE_RANGE","values":["30▶◀150"]}]
                shopee_min = min_value if min_bound is not None else 0
                shopee_max = max_value if max_bound is not None else 999999
                filter_payload = [
                    {
                        "group_name": "PRICE_RANGE",
                        "values": [f"{shopee_min}▶◀{shopee_max}"],
                    }
                ]
                query_params.append(f"fe_filter_options={quote_plus(json.dumps(filter_payload, ensure_ascii=False))}")

            elif site.get("range_mode") == "query_magalu_filter":
                # Magalu usa centavos inteiros (ex.: 6118 => R$ 61,18), então preservamos casas decimais.
                min_cents = int(round(min_bound * 100)) if min_bound is not None else 0
                max_cents = int(round(max_bound * 100)) if max_bound is not None else 999999999
                if max_cents < min_cents:
                    max_cents = min_cents
                query_params.append(f"filters=price---{min_cents}:{max_cents}")

        if not query_params:
            return url

        joiner = "&" if "?" in url else "?"
        return f"{url}{joiner}{'&'.join(query_params)}"

    def extract_price_from_card(self, card, selectors: Dict[str, str]) -> Optional[float]:
        whole_selector = selectors.get("price_whole", "")
        fraction_selector = selectors.get("price_fraction", "")

        whole_text = ""
        fraction_text = ""

        if whole_selector:
            whole_el = card.query_selector(whole_selector)
            if whole_el:
                whole_text = (whole_el.inner_text() or "").strip()

        if fraction_selector:
            fraction_el = card.query_selector(fraction_selector)
            if fraction_el:
                fraction_text = (fraction_el.inner_text() or "").strip()

        if whole_text:
            whole_digits = re.sub(r"\D", "", whole_text)
            fraction_digits = re.sub(r"\D", "", fraction_text)

            if whole_digits and fraction_digits:
                return parse_price_to_float(f"{whole_digits},{fraction_digits[:2]}")
            if whole_digits:
                return parse_price_to_float(whole_digits)

            if fraction_text:
                return parse_price_to_float(f"{whole_text},{fraction_text}")
            return parse_price_to_float(whole_text)

        return None

    def extract_image_from_card(self, card, selectors: Dict[str, str]) -> Optional[str]:
        image_selector = selectors.get("image", "")
        if not image_selector:
            return None

        image_el = card.query_selector(image_selector)
        if not image_el:
            return None

        for attr in ("src", "data-src", "data-srcset", "srcset"):
            raw_value = (image_el.get_attribute(attr) or "").strip()
            if not raw_value:
                continue
            image_url = extract_primary_image_url(raw_value)
            if image_url:
                return image_url

        return None

    def get_products(
        self,
        marketplace: str,
        keyword: str,
        limit: int,
        price_min: Optional[float] = None,
        price_max: Optional[float] = None,
        sort_by_price: bool = True,
        start_offset: int = 1,
    ) -> List[Product]:
        site_key = self.normalize_marketplace(marketplace)
        if site_key == "mercadolivre":
            return scrape_mercadolivre(
                keyword=keyword,
                limit=limit,
                price_min=price_min,
                price_max=price_max,
                sort_by_price=sort_by_price,
                start_offset=start_offset,
            )

        site = self.SITE_CONFIG[site_key]
        selectors = site["selectors"]
        search_url = self.build_search_url(
            marketplace=site_key,
            keyword=keyword,
            price_min=price_min,
            price_max=price_max,
            sort_by_price=sort_by_price,
            start_offset=start_offset,
        )

        products: List[Product] = []
        seen = set()
        logger.info("Abrindo busca %s: %s", site_key, search_url)

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
            context = self.browser_context_builder(browser)
            page = context.new_page()

            try:
                page.goto(search_url, wait_until="domcontentloaded", timeout=60000)
                page.wait_for_timeout(2500)
            except PlaywrightTimeoutError:
                logger.warning("Timeout ao abrir busca %s para '%s'.", site_key, keyword)
                context.close()
                browser.close()
                return []

            cards = page.query_selector_all(selectors.get("cards", ""))
            logger.info(
                "Busca %s para '%s' carregada | cards encontrados: %d",
                site_key,
                keyword,
                len(cards),
            )
            for card in cards:
                try:
                    link_el = card.query_selector(selectors.get("link", ""))
                    title_el = card.query_selector(selectors.get("title", ""))

                    if link_el and not (link_el.get_attribute("href") or "").strip():
                        nested_anchor = link_el.query_selector("a[href]")
                        if nested_anchor:
                            link_el = nested_anchor
                        else:
                            fallback_anchor = card.query_selector("a[href]")
                            if fallback_anchor:
                                link_el = fallback_anchor

                    if not link_el:
                        link_el = card.query_selector("a[href]")

                    if not title_el:
                        continue

                    raw_url = (link_el.get_attribute("href") or "").strip() if link_el else ""
                    if not raw_url:
                        raw_url = (card.evaluate(
                            """
                            (el) => {
                                const direct = el.querySelector('a[href]');
                                if (direct) return direct.getAttribute('href') || direct.href || '';

                                const parentAnchor = el.closest('a[href]');
                                if (parentAnchor) return parentAnchor.getAttribute('href') || parentAnchor.href || '';

                                const parent = el.parentElement;
                                if (parent) {
                                    const siblingAnchor = parent.querySelector('a[href]');
                                    if (siblingAnchor) return siblingAnchor.getAttribute('href') || siblingAnchor.href || '';
                                }
                                return '';
                            }
                            """
                        ) or "").strip()

                    if site_key == "amazon" and (raw_url.startswith("/dp/") or raw_url.startswith("/s?")):
                        raw_url = f"https://www.amazon.com.br{raw_url}"
                    elif site_key == "amazon" and raw_url.startswith("/"):
                        raw_url = f"https://www.amazon.com.br{raw_url}"
                    elif raw_url.startswith("/"):
                        # Dominio base por marketplace
                        base_domain = {
                            "shopee": "https://shopee.com.br",
                            "magalu": "https://www.magazineluiza.com.br",
                        }.get(site_key, "")
                        raw_url = f"{base_domain}{raw_url}" if base_domain else raw_url

                    url = normalize_url(raw_url)
                    title = (title_el.inner_text() or "").strip()

                    if not url or not title or url in seen:
                        continue

                    price = self.extract_price_from_card(card, selectors)
                    if price is None:
                        continue

                    image_url = self.extract_image_from_card(card, selectors)

                    seen.add(url)
                    products.append(Product(name=title, price=price, url=url, image_url=image_url))
                    if len(products) >= limit:
                        break
                except Exception as card_exc:
                    logger.debug("Falha ao extrair card em %s: %s", site_key, card_exc)
                    continue

            context.close()
            browser.close()

        sanitized_products = sanitize_products(products)
        logger.info(
            "Resultados %s para '%s' | urls unicas: %d | produtos extraidos: %d | produtos validos: %d",
            site_key,
            keyword,
            len(seen),
            len(products),
            len(sanitized_products),
        )

        return sanitized_products


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
    calibration_top_n = int(os.getenv("CALIBRATION_TOP_N", "50"))
    monitor_top_n = int(os.getenv("MONITOR_TOP_N", "5"))
    monitor_top_n = max(1, min(monitor_top_n, 5))
    min_price_threshold = float(os.getenv("MIN_PRICE_THRESHOLD", "0"))

    google_sheet_id = os.getenv("GOOGLE_SHEET_ID", "").strip()
    telegram_token = os.getenv("TELEGRAM_TOKEN", "").strip()
    telegram_chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()

    telegram_enabled = bool(telegram_token)

    if not google_sheet_id:
        raise ValueError("Defina a variável de ambiente GOOGLE_SHEET_ID.")

    return AppConfig(
        search_keyword=search_keyword,
        search_keywords=search_keywords,
        top_n=top_n,
        calibration_top_n=calibration_top_n,
        monitor_top_n=monitor_top_n,
        min_price_threshold=min_price_threshold,
        google_sheet_id=google_sheet_id,
        data_sheet_name=os.getenv("DATA_SHEET_NAME", "Historico").strip(),
        target_sheet_name=os.getenv("TARGET_SHEET_NAME", "PrecosAlvo").strip(),
        scraper_log_sheet_name=os.getenv("SCRAPER_LOG_SHEET_NAME", "LogScraper").strip() or "LogScraper",
        telegram_token=telegram_token,
        telegram_chat_id=telegram_chat_id,
        telegram_enabled=telegram_enabled,
    )


def parse_price_to_float(price_text: str) -> Optional[float]:
    if not price_text:
        logger.debug("Preco extraido: vazio -> Convertido: None")
        return None

    raw_text = str(price_text)
    cleaned = raw_text.strip().replace("R$", "").replace("\u00a0", "")
    cleaned = re.sub(r"\s+", "", cleaned)
    cleaned = re.sub(r"[^\d,\.]", "", cleaned)

    if not cleaned:
        logger.debug("Preco extraido: %s -> Convertido: None (sem digitos)", raw_text)
        return None

    normalized = cleaned
    has_comma = "," in normalized
    has_dot = "." in normalized

    if has_comma and has_dot:
        # Usa o ultimo separador como decimal para lidar com formatos mistos.
        if normalized.rfind(",") > normalized.rfind("."):
            normalized = normalized.replace(".", "").replace(",", ".")
        else:
            normalized = normalized.replace(",", "")
    elif has_comma:
        if re.search(r",\d{1,2}$", normalized):
            normalized = normalized.replace(".", "").replace(",", ".")
        else:
            normalized = normalized.replace(",", "")
    elif has_dot:
        if re.search(r"\.\d{1,2}$", normalized):
            normalized = normalized.replace(",", "")
        else:
            normalized = normalized.replace(".", "")

    if normalized.isdigit() and len(normalized) >= 5:
        as_int = int(normalized)
        # Alguns cards retornam centavos embutidos sem separador (ex.: 458247 => 4582.47).
        should_shift_cents = as_int >= 100000 or (as_int >= 10000 and as_int % 100 != 0)
        if should_shift_cents:
            normalized = f"{as_int / 100:.2f}"

    try:
        value = float(normalized)
        logger.debug("Preco extraido: %s -> Convertido: %.2f", raw_text, value)
        return value
    except ValueError:
        logger.debug("Preco extraido: %s -> Convertido: None (normalizado=%s)", raw_text, normalized)
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


def extract_primary_image_url(raw_value: str) -> Optional[str]:
    cleaned = (raw_value or "").strip()
    if not cleaned:
        return None

    first_chunk = cleaned.split(",", 1)[0].strip()
    candidate = first_chunk.split(" ", 1)[0].strip()
    if candidate.startswith("//"):
        candidate = f"https:{candidate}"
    return candidate if candidate.startswith("http") else None


def format_price_range_for_url(price_min: Optional[float], price_max: Optional[float]) -> str:
    if price_min is None or price_max is None:
        return ""

    min_int = max(0, int(round(price_min)))
    max_int = max(min_int, int(round(price_max)))
    return f"_PriceRange_{min_int}BRL-{max_int}BRL"


def build_marketplace_filters_suffix() -> str:
    item_condition = os.getenv("ML_ITEM_CONDITION", "").strip()
    shipping_origin = os.getenv("ML_SHIPPING_ORIGIN", "").strip()
    no_index_enabled = os.getenv("ML_NO_INDEX", "true").strip().lower() in {"1", "true", "yes"}

    suffix_parts: List[str] = []
    if item_condition:
        suffix_parts.append(f"_ITEM*CONDITION_{item_condition}")
    if no_index_enabled:
        suffix_parts.append("_NoIndex_True")
    if shipping_origin:
        suffix_parts.append(f"_SHIPPING*ORIGIN_{shipping_origin}")

    return "".join(suffix_parts)


def build_search_url(
    keyword: str,
    price_min: Optional[float] = None,
    price_max: Optional[float] = None,
    sort_by_price: bool = True,
    start_offset: int = 1,
) -> str:
    price_range_part = format_price_range_for_url(price_min, price_max)
    marketplace_filters = build_marketplace_filters_suffix()
    sort_part = "_OrderId_PRICE" if sort_by_price else ""
    offset_part = f"_Desde_{start_offset}" if start_offset > 1 else ""
    base = f"https://lista.mercadolivre.com.br/{quote_plus(keyword)}{offset_part}{sort_part}"
    return f"{base}{price_range_part}{marketplace_filters}"


def normalize_title_for_match(title: str) -> str:
    normalized = re.sub(r"[^\w\s]", " ", (title or "").strip().lower())
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized

def is_valid_marketplace_url(url: str) -> bool:
    if not url or not re.match(r"^https?://", url, re.IGNORECASE):
        return False

    allowed_hosts = [
        "mercadolivre.com.br",
        "amazon.com.br",
        "shopee.com.br",
        "magazineluiza.com.br",
    ]
    lowered = url.lower()
    return any(host in lowered for host in allowed_hosts)


def is_valid_mercadolivre_product_url(url: str) -> bool:
    return bool(url and re.search(r"/(MLB-|p/MLB)", url, re.IGNORECASE))


def is_valid_product_url(url: str) -> bool:
    if not url:
        return False

    normalized = normalize_url(url).lower()
    if not normalized.startswith("http"):
        return False

    if "mercadolivre." in normalized:
        return bool(re.search(r"/(MLB-|p/MLB)", normalized, re.IGNORECASE))

    if "amazon.com.br" in normalized:
        return bool(re.search(r"/(dp|gp/product)/", normalized, re.IGNORECASE))

    if "magazineluiza.com.br" in normalized:
        return "/p/" in normalized

    return True


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
        sanitized.append(Product(name=title, price=price, url=url, image_url=product.image_url))

    return sanitized


def normalize_text(text: str) -> str:
    ascii_text = unicodedata.normalize("NFKD", text or "").encode("ascii", "ignore").decode("ascii")
    normalized = re.sub(r"[^a-zA-Z0-9\s]", " ", ascii_text.lower())
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized


def parse_item_blacklist(raw_blacklist: str) -> List[str]:
    if not raw_blacklist:
        return []

    terms: List[str] = []
    seen = set()
    for part in re.split(r"[,;\n]+", str(raw_blacklist)):
        normalized = normalize_text(part)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        terms.append(normalized)
    return terms


def contains_item_blacklist_keyword(title: str, blacklist_terms: List[str]) -> bool:
    if not blacklist_terms:
        return False

    normalized_title = normalize_text(title)
    for term in blacklist_terms:
        if term and term in normalized_title:
            return True
    return False


def keyword_tokens(keyword: str) -> List[str]:
    return [token for token in normalize_text(keyword).split() if token]


def normalize_match_token(token: str) -> str:
    normalized = normalize_text(token).strip()
    if not normalized:
        return ""

    if normalized.isdigit() or len(normalized) <= 3:
        return normalized

    # Normalizacao leve de plural em PT-BR para melhorar recall (ex.: grao/graos).
    if normalized.endswith("oes") or normalized.endswith("aes"):
        return normalized[:-3] + "ao"
    if normalized.endswith("aos"):
        return normalized[:-1]
    if normalized.endswith("s") and not normalized.endswith("ss"):
        return normalized[:-1]

    return normalized


def normalized_keyword_token_set(text: str) -> set:
    normalized_tokens = {
        normalize_match_token(token)
        for token in keyword_tokens(text)
    }
    normalized_tokens.discard("")
    return normalized_tokens


def extract_normalized_quantities(text: str) -> List[str]:
    normalized = unicodedata.normalize("NFKD", text or "").encode("ascii", "ignore").decode("ascii").lower()
    quantities: List[str] = []
    seen = set()

    for amount_raw, unit_raw in QUANTITY_PATTERN.findall(normalized):
        try:
            amount = float(amount_raw.replace(",", "."))
        except ValueError:
            continue

        unit = unit_raw.lower()
        if unit in {"kg", "g"}:
            value = int(round(amount * 1000)) if unit == "kg" else int(round(amount))
            key = f"mass:{value}"
        else:
            value = int(round(amount * 1000)) if unit == "l" else int(round(amount))
            key = f"volume:{value}"

        if value <= 0 or key in seen:
            continue
        seen.add(key)
        quantities.append(key)

    return quantities


def matches_keyword_quantities(keyword: str, title: str) -> bool:
    required_quantities = extract_normalized_quantities(keyword)
    if not required_quantities:
        return True

    title_quantities = set(extract_normalized_quantities(title))
    return all(qty in title_quantities for qty in required_quantities)


def keyword_core_tokens(keyword: str) -> List[str]:
    # Remove explicit quantity markers to avoid duplicate validation with quantity matching.
    keyword_wo_qty = QUANTITY_PATTERN.sub(" ", keyword or "")
    return [token for token in normalize_text(keyword_wo_qty).split() if token]


def contains_blacklist_word(title: str) -> bool:
    normalized_title = normalize_text(title)
    if not normalized_title:
        return False

    title_tokens = set(normalized_title.split())
    return any(normalize_text(word) in title_tokens for word in BLACKLIST_WORDS)


def has_suspicious_preposition_before_keyword(keyword: str, title: str) -> bool:
    normalized_title = normalize_text(title)
    keyword_parts = keyword_tokens(keyword)
    if not normalized_title or not keyword_parts:
        return False

    anchor = keyword_parts[0]
    anchor_match = re.search(rf"\b{re.escape(anchor)}\b", normalized_title)
    if not anchor_match:
        return False

    prefix = normalized_title[:anchor_match.start()].strip()
    if not prefix:
        return False

    return any(re.search(rf"\b{re.escape(term)}\b", prefix) for term in PREPOSITION_GUARD_TERMS)


def validate_title_match(keyword: str, title: str) -> bool:
    if contains_blacklist_word(title):
        return False
    if has_suspicious_preposition_before_keyword(keyword, title):
        return False
    if not matches_keyword_quantities(keyword, title):
        return False

    required_tokens = {
        normalize_match_token(token)
        for token in keyword_core_tokens(keyword)
        if normalize_match_token(token)
    }
    if not required_tokens:
        return True

    title_tokens = normalized_keyword_token_set(title)
    return all(token in title_tokens for token in required_tokens)


def filter_valid_products(
    products: List[Product],
    search_keyword: str,
    min_price_threshold: float,
    item_blacklist_terms: Optional[List[str]] = None,
) -> List[Product]:
    base = sanitize_products(products)
    validated: List[Product] = []
    scoped_blacklist_terms = item_blacklist_terms or []

    for product in base:
        if product.price < min_price_threshold:
            continue
        if contains_item_blacklist_keyword(product.name, scoped_blacklist_terms):
            logger.debug(
                "Produto descartado por blacklist local | termo='%s' | titulo='%s'",
                search_keyword,
                product.name,
            )
            continue
        if not validate_title_match(search_keyword, product.name):
            continue
        validated.append(product)

    return validated


def filter_products_by_price_range(
    products: List[Product],
    price_min: Optional[float],
    price_max: Optional[float],
) -> List[Product]:
    if price_min is None or price_max is None:
        return products

    return [product for product in products if price_min <= product.price <= price_max]


def scrape_mercadolivre_api(
    keyword: str,
    limit: int,
    price_min: Optional[float] = None,
    price_max: Optional[float] = None,
    sort_by_price: bool = True,
) -> List[Product]:
    endpoint = "https://api.mercadolibre.com/sites/MLB/search"
    logger.info("Tentando fallback pela API pública do Mercado Livre.")

    session = get_requests_session()

    try:
        params = {"q": keyword, "limit": limit}
        if sort_by_price:
            params["sort"] = "price_asc"
        if price_min is not None and price_max is not None:
            min_int = max(0, int(round(price_min)))
            max_int = max(min_int, int(round(price_max)))
            params["price"] = f"{min_int}-{max_int}"

        response = session.get(
            endpoint,
            params=params,
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

        image_url = extract_primary_image_url(
            str(item.get("thumbnail", "") or item.get("secure_thumbnail", "") or "")
        )

        products.append(Product(name=title, price=price, url=permalink, image_url=image_url))

        if len(products) >= limit:
            break

    products = sanitize_products(products)
    logger.info("Produtos coletados via API (sanitizados): %d", len(products))
    return products


def scrape_mercadolivre_http(
    keyword: str,
    limit: int,
    price_min: Optional[float] = None,
    price_max: Optional[float] = None,
    sort_by_price: bool = True,
    start_offset: int = 1,
) -> List[Product]:
    logger.info("Tentando fallback por HTML via requests.")
    search_url = build_search_url(
        keyword,
        price_min=price_min,
        price_max=price_max,
        sort_by_price=sort_by_price,
        start_offset=start_offset,
    )

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
        image_url = None

        ldjson_products = extract_products_from_ldjson(page_html, limit=1)
        if ldjson_products:
            title = ldjson_products[0].get("title")
            price = parse_price_to_float(ldjson_products[0].get("price_text") or "")
            image_url = ldjson_products[0].get("image_url")

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

        if not image_url:
            og_image_match = re.search(
                r'<meta[^>]+property=["\']og:image["\'][^>]+content=["\']([^"\']+)["\']',
                page_html,
                flags=re.IGNORECASE,
            )
            if og_image_match:
                image_url = extract_primary_image_url(og_image_match.group(1))

        if title and price is not None:
            products.append(Product(name=title, price=price, url=url, image_url=image_url))

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
                image_url = None

                offers = item.get("offers")
                if isinstance(offers, dict):
                    price = str(offers.get("price", "") or "").strip()

                image_data = item.get("image")
                if isinstance(image_data, str):
                    image_url = extract_primary_image_url(image_data)
                elif isinstance(image_data, list):
                    for image_item in image_data:
                        if isinstance(image_item, str):
                            image_url = extract_primary_image_url(image_item)
                            if image_url:
                                break
                if not url or url in seen or not is_valid_product_url(url):
                    continue

                products.append({"url": url, "title": name, "price_text": price, "image_url": image_url})
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


def scrape_top_product_links(
    keyword: str,
    limit: int,
    price_min: Optional[float] = None,
    price_max: Optional[float] = None,
    validate_with_keyword: Optional[str] = None,
    sort_by_price: bool = True,
    start_offset: int = 1,
) -> List[Dict[str, str]]:
    search_url = build_search_url(
        keyword,
        price_min=price_min,
        price_max=price_max,
        sort_by_price=sort_by_price,
        start_offset=start_offset,
    )
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

        ldjson_products = extract_products_from_ldjson(html, 200)
        if ldjson_products:
            for item in ldjson_products:
                url = normalize_url(str(item.get("url", "") or ""))
                if not url or url in seen_urls:
                    continue
                if url.startswith("/"):
                    url = f"https://www.mercadolivre.com.br{url}"
                if not is_valid_mercadolivre_product_url(url):
                    continue

                title = (str(item.get("title", "") or "").strip() or "Produto")
                if validate_with_keyword and not validate_title_match(validate_with_keyword, title):
                    continue

                products.append(
                    {
                        "url": url,
                        "title": title,
                        "price_text": str(item.get("price_text", "") or "").strip() or None,
                        "image_url": item.get("image_url"),
                    }
                )
                seen_urls.add(url)

                if len(products) >= limit:
                    context.close()
                    browser.close()
                    logger.info("URLs válidas coletadas via JSON-LD: %d", len(products))
                    return products

        cards = page.query_selector_all("li.ui-search-layout__item, div.poly-card")

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
                if not is_valid_mercadolivre_product_url(url):
                    continue

                title = (str(link.get("text", "") or "").strip() or "Produto")
                if validate_with_keyword and not validate_title_match(validate_with_keyword, title):
                    continue

                products.append({"url": url, "title": title, "price_text": None, "image_url": None})
                seen_urls.add(url)
                if len(products) >= limit:
                    break

            context.close()
            browser.close()
            logger.info("URLs coletadas (fallback): %d", len(products))
            return products

        for card in cards:
            link_el = (
                card.query_selector("a.ui-search-link")
                or card.query_selector("a.poly-component__title")
                or card.query_selector("a.poly-card__title")
                or card.query_selector("h3 a")
            )
            title_el = card.query_selector("h3") or card.query_selector("a.poly-component__title")
            fraction_el = card.query_selector("span.andes-money-amount__fraction")
            cents_el = card.query_selector("span.andes-money-amount__cents")

            if not link_el:
                continue

            url = link_el.get_attribute("href") or ""
            url = normalize_url(url)

            if not url or url in seen_urls:
                continue

            if not is_valid_mercadolivre_product_url(url):
                continue

            title = (title_el.inner_text().strip() if title_el else "Produto")
            if validate_with_keyword and not validate_title_match(validate_with_keyword, title):
                continue

            price_text = None
            if fraction_el:
                fraction = (fraction_el.inner_text() or "").strip()
                cents = (cents_el.inner_text() or "00").strip() if cents_el else "00"
                if fraction:
                    price_text = f"{fraction},{cents or '00'}"

            image_el = card.query_selector("img")
            image_raw = ""
            if image_el:
                image_raw = (
                    image_el.get_attribute("src")
                    or image_el.get_attribute("data-src")
                    or image_el.get_attribute("srcset")
                    or ""
                )
            image_url = extract_primary_image_url(image_raw)

            products.append({"url": url, "title": title, "price_text": price_text, "image_url": image_url})
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
            "args": [
                "--disable-blink-features=AutomationControlled", 
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-dev-shm-usage",
                "--disable-gpu"
            ],
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

        image_url = None
        image_selectors = [
            "meta[property='og:image']",
            "img.ui-pdp-image",
            "img.andes-carousel-snapped__figure__image",
            "img",
        ]
        for selector in image_selectors:
            el = page.query_selector(selector)
            if not el:
                continue

            raw_value = (el.get_attribute("content") or el.get_attribute("src") or "").strip()
            image_url = extract_primary_image_url(raw_value)
            if image_url:
                break

        context.close()
        browser.close()

        if price_value is None:
            logger.warning("Não foi possível extrair preço em: %s", url)
            return None

        return Product(name=title, price=price_value, url=url, image_url=image_url)


def scrape_mercadolivre(
    keyword: str,
    limit: int,
    price_min: Optional[float] = None,
    price_max: Optional[float] = None,
    sort_by_price: bool = True,
    start_offset: int = 1,
) -> List[Product]:
    links = scrape_top_product_links(
        keyword,
        limit,
        price_min=price_min,
        price_max=price_max,
        validate_with_keyword=keyword,
        sort_by_price=sort_by_price,
        start_offset=start_offset,
    )

    if not links:
        logger.warning("Sem links pela interface web. Usando fallback API.")
        api_items = scrape_mercadolivre_api(
            keyword,
            limit,
            price_min=price_min,
            price_max=price_max,
            sort_by_price=sort_by_price,
        )
        if api_items:
            return sanitize_products(api_items)
        logger.warning("Fallback API sem resultados. Tentando fallback HTML requests.")
        return sanitize_products(
            scrape_mercadolivre_http(
                keyword,
                limit,
                price_min=price_min,
                price_max=price_max,
                sort_by_price=sort_by_price,
                start_offset=start_offset,
            )
        )

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
                Product(
                    name=entry["title"],
                    price=fallback_price,
                    url=entry["url"],
                    image_url=entry.get("image_url"),
                )
            )

    if not items:
        logger.warning("Sem produtos válidos pelo Playwright. Usando fallback API.")
        api_items = scrape_mercadolivre_api(
            keyword,
            limit,
            price_min=price_min,
            price_max=price_max,
            sort_by_price=sort_by_price,
        )
        if api_items:
            return sanitize_products(api_items)
        logger.warning("Fallback API sem resultados. Tentando fallback HTML requests.")
        return sanitize_products(
            scrape_mercadolivre_http(
                keyword,
                limit,
                price_min=price_min,
                price_max=price_max,
                sort_by_price=sort_by_price,
                start_offset=start_offset,
            )
        )

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


def enviar_alerta_telegram(
    chat_id: str,
    produto: str,
    preco: float,
    mediana: float,
    desconto: float,
    link: str,
    image_url: Optional[str],
) -> None:
    token = os.getenv("TELEGRAM_TOKEN", "").strip()
    if not token or not chat_id:
        logger.info("Telegram não configurado. Alerta ignorado.")
        return

    discount_pct = desconto * 100 if abs(desconto) <= 1 else desconto
    safe_produto = escape(produto or "")
    safe_link = escape(link or "")
    caption = (
        "🎉 <b>Achado do Dia!</b>\n"
        f"🛒 <b>Produto:</b> <i>{safe_produto}</i>\n\n"
        f"💰 <b>Preço Agora:</b> R$ <b>{brl(preco)}</b>\n"
        f"📉 <b>Preço Médio:</b> R$ <b>{brl(mediana)}</b>\n"
        f"🔥 <b>Desconto:</b> {discount_pct:.2f}% OFF!\n\n"
        f"🔗 <a href=\"{safe_link}\">Clique aqui para aproveitar!</a>"
    )
    if len(caption) > 1024:
        caption = caption[:1021] + "..."

    if image_url:
        endpoint = f"https://api.telegram.org/bot{token}/sendPhoto"
        response = requests.post(
            endpoint,
            json={
                "chat_id": chat_id,
                "photo": image_url,
                "caption": caption,
                "parse_mode": "HTML",
            },
            timeout=30,
        )
    else:
        endpoint = f"https://api.telegram.org/bot{token}/sendMessage"
        response = requests.post(
            endpoint,
            json={
                "chat_id": chat_id,
                "text": caption,
                "parse_mode": "HTML",
                "disable_web_page_preview": False,
            },
            timeout=30,
        )

    if response.status_code >= 300:
        raise RuntimeError(f"Erro no Telegram: {response.status_code} - {response.text}")


def open_spreadsheet(config: AppConfig):
    gc = get_gspread_client()
    try:
        return gc.open_by_key(config.google_sheet_id)
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


def ensure_history_headers(data_ws) -> None:
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


def ensure_target_headers(target_ws) -> None:
    expected_headers = [
        "Chat_ID",
        "Termo Buscado",
        "Marketplace",
        "Preco Minimo",
        "Preco Maximo",
        "Data Ultima Calibragem",
        "Blacklist",
    ]
    current_headers = target_ws.row_values(1)
    if current_headers != expected_headers:
        target_ws.update("A1:G1", [expected_headers], value_input_option="USER_ENTERED")


def ensure_baseline_headers(baseline_ws) -> None:
    expected_headers = [
        "Chat_ID",
        "Termo Buscado",
        "Marketplace",
        "Preco Minimo",
        "Preco Maximo",
        "Data Ultima Calibragem",
    ]
    current_headers = baseline_ws.row_values(1)
    if current_headers != expected_headers:
        baseline_ws.update("A1:F1", [expected_headers], value_input_option="USER_ENTERED")


def ensure_scraper_log_headers(log_ws) -> None:
    expected_headers = [
        "Data/Hora",
        "Execucao_ID",
        "Chat_ID",
        "Termo Buscado",
        "Marketplace",
        "Pagina",
        "Titulo",
        "Preco",
        "URL",
        "Imagem",
        "Passou_Filtro_Estrito",
        "Passou_Filtro_Final",
    ]
    current_headers = log_ws.row_values(1)
    if current_headers != expected_headers:
        log_ws.update("A1:L1", [expected_headers], value_input_option="USER_ENTERED")


def append_scraper_logs(log_ws, rows: List[List[Any]]) -> None:
    if not rows:
        return

    ensure_scraper_log_headers(log_ws)
    log_ws.append_rows(rows, value_input_option="USER_ENTERED")


def get_or_create_worksheet(sh, worksheet_name: str, rows: int = 200, cols: int = 10):
    try:
        return sh.worksheet(worksheet_name)
    except gspread.exceptions.WorksheetNotFound:
        logger.warning("A aba '%s' não existe. Criando automaticamente.", worksheet_name)
        return sh.add_worksheet(title=worksheet_name, rows=rows, cols=cols)


def get_baseline_for_keyword(baseline_ws, search_keyword: str, marketplace: str, chat_id: Optional[str] = None):
    rows = baseline_ws.get_all_records()
    keyword_key = search_keyword.strip().lower()
    marketplace_key = (marketplace or DEFAULT_MARKETPLACE).strip().lower()
    target_chat_id = str(chat_id or "").strip()

    for idx, row in enumerate(rows, start=2):
        row_chat_id = str(row.get("Chat_ID", "") or "").strip()
        term = str(row.get("Termo Buscado", "") or "").strip().lower()
        row_marketplace = str(row.get("Marketplace", "") or "").strip().lower() or DEFAULT_MARKETPLACE
        if target_chat_id and row_chat_id != target_chat_id:
            continue
        if term == keyword_key and row_marketplace == marketplace_key:
            return idx, row

    return None, None


def parse_calibration_date(value: str) -> Optional[datetime]:
    raw = str(value or "").strip()
    if not raw:
        return None

    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(raw, fmt)
        except ValueError:
            continue
    return None


def upsert_market_baseline(
    baseline_ws,
    search_keyword: str,
    marketplace: str,
    price_min: float,
    price_max: float,
    calibration_timestamp: str,
    chat_id: str = "GLOBAL",
) -> None:
    ensure_baseline_headers(baseline_ws)
    row_index, _ = get_baseline_for_keyword(baseline_ws, search_keyword, marketplace, chat_id=chat_id)
    payload = [chat_id, search_keyword, marketplace, price_min, price_max, calibration_timestamp]

    if row_index is None:
        baseline_ws.append_row(payload, value_input_option="USER_ENTERED")
        logger.info("Baseline inserido para termo: %s", search_keyword)
        return

    baseline_ws.update(
        f"A{row_index}:F{row_index}",
        [payload],
        value_input_option="USER_ENTERED",
    )
    logger.info("Baseline atualizado para termo: %s [%s]", search_keyword, marketplace)


def calibrate_market_baseline(
    scraper: MultiMarketplaceScraper,
    config: AppConfig,
    baseline_ws,
    search_keyword: str,
    marketplace: str,
):
    logger.info("Calibrando baseline para termo: %s [%s]", search_keyword, marketplace)
    products = scraper.get_products(
        marketplace=marketplace,
        keyword=search_keyword,
        limit=config.calibration_top_n,
        sort_by_price=False,
    )
    valid_products = filter_valid_products(
        products=products,
        search_keyword=search_keyword,
        min_price_threshold=config.min_price_threshold,
    )

    if not valid_products:
        logger.warning("Calibragem sem produtos válidos para o termo: %s", search_keyword)
        return None

    max_price = max(product.price for product in valid_products)
    relative_floor = max_price * 0.50
    robust_products = [
        product for product in valid_products if product.price >= relative_floor
    ]

    if robust_products:
        valid_products = robust_products
    else:
        logger.warning(
            "Filtro de robustez removeu todos os itens de calibragem para '%s'. Mantendo lista validada original.",
            search_keyword,
        )

    df = pd.DataFrame(
        [{"name": p.name, "price": p.price, "url": p.url} for p in valid_products]
    )
    if df.empty:
        logger.warning("Calibragem retornou DataFrame vazio para o termo: %s", search_keyword)
        return None

    median_price = float(df["price"].median())
    baseline_min = median_price * 0.50
    baseline_max = median_price * 1.10
    timestamp = now_brt_str()

    upsert_market_baseline(
        baseline_ws=baseline_ws,
        search_keyword=search_keyword,
        marketplace=marketplace,
        price_min=baseline_min,
        price_max=baseline_max,
        calibration_timestamp=timestamp,
    )

    return {
        "median": median_price,
        "min": baseline_min,
        "max": baseline_max,
    }


def calibrate_global_market_baseline(
    scraper: MultiMarketplaceScraper,
    config: AppConfig,
    baseline_ws,
    search_keyword: str,
):
    logger.info("Calibrando baseline GLOBAL para termo: %s", search_keyword)
    global_pool: List[Dict[str, Any]] = []

    for marketplace in scraper.SITE_CONFIG.keys():
        products = scraper.get_products(
            marketplace=marketplace,
            keyword=search_keyword,
            limit=config.calibration_top_n,
            sort_by_price=False,
        )
        valid_products = filter_valid_products(
            products=products,
            search_keyword=search_keyword,
            min_price_threshold=config.min_price_threshold,
        )

        for product in valid_products:
            global_pool.append(
                {
                    "store": marketplace,
                    "title": product.name,
                    "price": product.price,
                    "url": product.url,
                }
            )

    if not global_pool:
        logger.warning("Calibragem GLOBAL sem produtos válidos para o termo: %s", search_keyword)
        return None

    df = pd.DataFrame(global_pool)
    if df.empty:
        logger.warning("Calibragem GLOBAL retornou DataFrame vazio para o termo: %s", search_keyword)
        return None

    max_price = float(df["price"].max())
    robust_floor = max_price * 0.50
    df = df[df["price"] >= robust_floor]
    if df.empty:
        logger.warning("Filtro de robustez GLOBAL removeu todos os itens para: %s", search_keyword)
        return None

    median_price = float(df["price"].median())
    baseline_min = median_price * 0.50
    baseline_max = median_price * 1.10
    timestamp = now_brt_str()

    upsert_market_baseline(
        baseline_ws=baseline_ws,
        search_keyword=search_keyword,
        marketplace="global",
        price_min=baseline_min,
        price_max=baseline_max,
        calibration_timestamp=timestamp,
    )

    return {
        "median": median_price,
        "min": baseline_min,
        "max": baseline_max,
    }


def process_products(
    config: AppConfig,
    data_ws,
    products: List[Product],
    search_keyword: str,
    alert_chat_id: Optional[str] = None,
) -> bool:
    ensure_history_headers(data_ws)

    if not products:
        logger.warning("Nenhum produto válido para processar no termo: %s", search_keyword)
        return False

    df = pd.DataFrame(
        [{"name": p.name, "price": p.price, "url": p.url, "image_url": p.image_url} for p in products]
    )

    if df.empty:
        logger.warning("DataFrame vazio após processamento.")
        return False

    min_idx = df["price"].idxmin()
    current_price = float(df.loc[min_idx, "price"])
    median_price = float(df["price"].median())
    current_link = str(df.loc[min_idx, "url"])
    current_image_url = str(df.loc[min_idx, "image_url"] or "").strip() or None
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
        median_price,
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
        median_reference = safe_float(existing_row.get("Preço Médio"))
        if median_reference is None or median_reference <= 0:
            median_reference = median_price

        discount_pct = 0.0
        if median_reference and median_reference > 0:
            discount_pct = (median_reference - current_price) / median_reference

        if discount_pct >= 0.10:
            old_reference = previous_historical_min if previous_historical_min is not None else historical_min
            message = (
                f"📉 Novo recorde de preço para '{search_keyword}'!\n"
                f"Preço atual: R$ {brl(current_price)}\n"
                f"Mediana referência: R$ {brl(median_reference)}\n"
                f"Desconto vs mediana: {discount_pct * 100:.2f}%\n"
                f"Recorde anterior: R$ {brl(old_reference)}\n"
                f"Variação histórica: {variation_pct:.2f}%\n"
                f"Link: {current_link}"
            )
            if config.telegram_enabled:
                enviar_alerta_telegram(
                    chat_id=str(alert_chat_id or "").strip(),
                    produto=search_keyword,
                    preco=current_price,
                    mediana=median_reference,
                    desconto=discount_pct,
                    link=current_link,
                    image_url=current_image_url,
                )
                logger.info(
                    "Alerta enviado para '%s' | desconto vs mediana %.2f%%",
                    search_keyword,
                    discount_pct * 100,
                )
            else:
                logger.info("Telegram desabilitado. Mensagem gerada: %s", message)
        else:
            logger.info(
                "Desconto insuficiente para alerta em '%s': %.2f%% (< 10%%).",
                search_keyword,
                discount_pct * 100,
            )

    return True


def collect_monitor_products_with_quota(
    scraper: MultiMarketplaceScraper,
    config: AppConfig,
    search_keyword: str,
    marketplace: str,
    price_min: float,
    price_max: float,
    quota: int,
    max_pages: int = 3,
    item_blacklist_terms: Optional[List[str]] = None,
    alert_chat_id: str = "",
    execution_id: str = "",
) -> Dict[str, Any]:
    collected: List[Product] = []
    seen_urls = set()
    log_rows: List[List[Any]] = []

    for page_number in range(1, max_pages + 1):
        start_offset = 1 + (page_number - 1) * 48
        logger.info(
            "Monitoramento '%s' [%s] | página %d/%d | offset=%d",
            search_keyword,
            marketplace,
            page_number,
            max_pages,
            start_offset,
        )

        page_products = scraper.get_products(
            marketplace=marketplace,
            keyword=search_keyword,
            limit=quota,
            price_min=price_min,
            price_max=price_max,
            sort_by_price=True,
            start_offset=start_offset,
        )

        page_valid_strict = filter_valid_products(
            products=page_products,
            search_keyword=search_keyword,
            min_price_threshold=config.min_price_threshold,
            item_blacklist_terms=item_blacklist_terms,
        )

        page_valid = filter_products_by_price_range(page_valid_strict, price_min, price_max)

        if not page_valid and page_products:
            # Fallback controlado: mantém itens válidos por URL/preço/faixa quando a validação de título elimina tudo.
            relaxed_base = sanitize_products(page_products)
            relaxed_base = [p for p in relaxed_base if p.price >= config.min_price_threshold]
            relaxed_base = [p for p in relaxed_base if matches_keyword_quantities(search_keyword, p.name)]
            if item_blacklist_terms:
                relaxed_base = [
                    p for p in relaxed_base if not contains_item_blacklist_keyword(p.name, item_blacklist_terms)
                ]
            relaxed_valid = filter_products_by_price_range(relaxed_base, price_min, price_max)

            if relaxed_valid:
                logger.warning(
                    "Monitoramento '%s' [%s] | fallback semântico ativado: estrito=0, relaxado=%d",
                    search_keyword,
                    marketplace,
                    len(relaxed_valid),
                )
                page_valid = relaxed_valid

        strict_urls = {normalize_url(product.url) for product in page_valid_strict if product.url}
        final_urls = {normalize_url(product.url) for product in page_valid if product.url}

        for product in page_products:
            norm_url = normalize_url(product.url)
            log_rows.append(
                [
                    now_brt_str(),
                    execution_id,
                    str(alert_chat_id or "").strip(),
                    search_keyword,
                    marketplace,
                    page_number,
                    product.name,
                    float(product.price),
                    norm_url,
                    product.image_url or "",
                    "sim" if norm_url in strict_urls else "nao",
                    "sim" if norm_url in final_urls else "nao",
                ]
            )

        for product in page_valid:
            norm_url = normalize_url(product.url)
            if not norm_url or norm_url in seen_urls:
                continue
            seen_urls.add(norm_url)
            collected.append(product)

            if len(collected) >= quota:
                return {"products": collected, "log_rows": log_rows}

    return {"products": collected, "log_rows": log_rows}


async def fetch_all_marketplaces(
    scraper: MultiMarketplaceScraper,
    config: AppConfig,
    product_name: str,
    discount_step: float,
    baseline_median: float,
    price_cap: float,
    item_blacklist_terms: Optional[List[str]] = None,
    alert_chat_id: str = "",
    execution_id: str = "",
) -> Dict[str, Any]:
    dynamic_floor = max(baseline_median * (1 - discount_step), config.min_price_threshold)
    marketplaces = list(scraper.SITE_CONFIG.keys())

    tasks = [
        asyncio.to_thread(
            collect_monitor_products_with_quota,
            scraper,
            config,
            product_name,
            marketplace,
            dynamic_floor,
            price_cap,
            config.monitor_top_n,
            3,
            item_blacklist_terms,
            alert_chat_id,
            execution_id,
        )
        for marketplace in marketplaces
    ]

    results = await asyncio.gather(*tasks, return_exceptions=True)
    global_pool: List[Dict[str, Any]] = []
    all_log_rows: List[List[Any]] = []

    for marketplace, result in zip(marketplaces, results):
        if isinstance(result, Exception):
            logger.warning("Falha na busca %s para '%s': %s", marketplace, product_name, result)
            continue

        if not isinstance(result, dict):
            logger.warning("Coleta GLOBAL '%s' | loja=%s retornou tipo inesperado: %s", product_name, marketplace, type(result).__name__)
            continue

        result_products = result.get("products", [])
        result_logs = result.get("log_rows", [])
        if isinstance(result_logs, list):
            all_log_rows.extend(result_logs)

        if not isinstance(result_products, list):
            logger.warning("Coleta GLOBAL '%s' | loja=%s retornou lista de produtos inválida: %s", product_name, marketplace, type(result_products).__name__)
            continue

        store_items: List[Dict[str, Any]] = []
        for product in result_products:
            price_value = safe_float(getattr(product, "price", None))
            title_value = str(getattr(product, "name", "") or "").strip()
            url_value = str(getattr(product, "url", "") or "").strip()
            image_value = getattr(product, "image_url", None)

            if price_value is None:
                logger.debug("Coleta GLOBAL '%s' | loja=%s descartou item sem preco valido: title=%s raw_price=%s", product_name, marketplace, title_value, getattr(product, "price", None))
                continue
            if not title_value or not url_value:
                logger.debug("Coleta GLOBAL '%s' | loja=%s descartou item incompleto: title=%s url=%s", product_name, marketplace, title_value, url_value)
                continue

            store_items.append(
                {
                    "store": marketplace,
                    "title": title_value,
                    "price": float(price_value),
                    "url": url_value,
                    "image_url": image_value,
                }
            )

        logger.info(
            "Coleta GLOBAL '%s' | loja=%s | produtos recebidos: %d | produtos consolidados: %d | piso=%.2f teto=%.2f",
            product_name,
            marketplace,
            len(result_products),
            len(store_items),
            dynamic_floor,
            price_cap,
        )

        global_pool.extend(store_items)

    logger.info("Coleta GLOBAL '%s' | total consolidado no pool: %d", product_name, len(global_pool))

    return {"global_pool": global_pool, "log_rows": all_log_rows}


def daily_monitor(
    scraper: MultiMarketplaceScraper,
    config: AppConfig,
    data_ws,
    baseline_ws,
    scraper_log_ws,
    search_keyword: str,
    alert_chat_id: str,
    item_blacklist_terms: Optional[List[str]] = None,
    execution_id: str = "",
) -> bool:
    ensure_baseline_headers(baseline_ws)
    ensure_history_headers(data_ws)

    _, baseline_row = get_baseline_for_keyword(baseline_ws, search_keyword, "global", chat_id="GLOBAL")
    price_min = safe_float(baseline_row.get("Preco Minimo")) if baseline_row else None
    price_max = safe_float(baseline_row.get("Preco Maximo")) if baseline_row else None
    last_calibration_raw = baseline_row.get("Data Ultima Calibragem") if baseline_row else ""
    last_calibration_at = parse_calibration_date(str(last_calibration_raw or ""))

    baseline_expired = True
    if last_calibration_at is not None:
        baseline_expired = (datetime.now() - last_calibration_at) > timedelta(days=30)

    if baseline_row and baseline_expired:
        logger.info(
            "Calibragem GLOBAL de '%s' com mais de 30 dias. Recalibrando por relevância.",
            search_keyword,
        )
        baseline = calibrate_global_market_baseline(
            scraper,
            config,
            baseline_ws,
            search_keyword,
        )
        if not baseline:
            return False
        price_min = baseline["min"]
        price_max = baseline["max"]

    if price_min is None or price_max is None or price_max <= price_min:
        logger.warning(
            "Baseline GLOBAL ausente/inválido para '%s'. Iniciando calibragem.",
            search_keyword,
        )
        baseline = calibrate_global_market_baseline(
            scraper,
            config,
            baseline_ws,
            search_keyword,
        )
        if not baseline:
            return False
        price_min = baseline["min"]
        price_max = baseline["max"]

    logger.info(
        "Monitoramento GLOBAL '%s' com faixa de preço %.2f - %.2f.",
        search_keyword,
        price_min,
        price_max,
    )
    discount_steps = [0.50, 0.40, 0.30, 0.20, 0.10]

    baseline_median_candidates = []
    if price_min is not None and price_min > 0:
        baseline_median_candidates.append(price_min / 0.50)
    if price_max is not None and price_max > 0:
        baseline_median_candidates.append(price_max / 1.10)

    if not baseline_median_candidates:
        logger.warning(
            "Não foi possível inferir mediana de baseline GLOBAL para '%s'. Recalibrando.",
            search_keyword,
        )
        baseline = calibrate_global_market_baseline(
            scraper,
            config,
            baseline_ws,
            search_keyword,
        )
        if not baseline:
            return False
        price_max = baseline["max"]
        baseline_median = baseline["median"]
    else:
        baseline_median = sum(baseline_median_candidates) / len(baseline_median_candidates)

    global_pool: List[Dict[str, Any]] = []
    for idx, step in enumerate(discount_steps):
        dynamic_floor = max(baseline_median * (1 - step), config.min_price_threshold)

        logger.info(
            "Tentando monitoramento GLOBAL '%s' com desconto %.0f%% | piso %.2f | teto %.2f",
            search_keyword,
            step * 100,
            dynamic_floor,
            price_max,
        )

        fetch_result = asyncio.run(
            fetch_all_marketplaces(
                scraper=scraper,
                config=config,
                product_name=search_keyword,
                discount_step=step,
                baseline_median=baseline_median,
                price_cap=price_max,
                item_blacklist_terms=item_blacklist_terms,
                alert_chat_id=alert_chat_id,
                execution_id=execution_id,
            )
        )
        global_pool = fetch_result.get("global_pool", []) if isinstance(fetch_result, dict) else []
        log_rows = fetch_result.get("log_rows", []) if isinstance(fetch_result, dict) else []
        append_scraper_logs(scraper_log_ws, log_rows)

        if global_pool:
            break

        if idx < len(discount_steps) - 1:
            next_step = discount_steps[idx + 1]
            logger.warning(
                "Pool vazio a %.0f%%. Reduzindo desconto para %.0f%%.",
                step * 100,
                next_step * 100,
            )

    if not global_pool:
        logger.warning(
            "Sem resultados válidos após relaxação progressiva GLOBAL para '%s'. Recalibrando de forma definitiva.",
            search_keyword,
        )
        baseline = calibrate_global_market_baseline(
            scraper=scraper,
            config=config,
            baseline_ws=baseline_ws,
            search_keyword=search_keyword,
        )
        if not baseline:
            return False

        fetch_result = asyncio.run(
            fetch_all_marketplaces(
                scraper=scraper,
                config=config,
                product_name=search_keyword,
                discount_step=discount_steps[0],
                baseline_median=baseline["median"],
                price_cap=baseline["max"],
                item_blacklist_terms=item_blacklist_terms,
                alert_chat_id=alert_chat_id,
                execution_id=execution_id,
            )
        )
        global_pool = fetch_result.get("global_pool", []) if isinstance(fetch_result, dict) else []
        log_rows = fetch_result.get("log_rows", []) if isinstance(fetch_result, dict) else []
        append_scraper_logs(scraper_log_ws, log_rows)

    if not global_pool:
        logger.warning("Mesmo após recalibragem GLOBAL, sem resultados válidos para '%s'.", search_keyword)
        return False

    global_df = pd.DataFrame(global_pool)
    if global_df.empty:
        logger.warning("Pool GLOBAL vazio para '%s'.", search_keyword)
        return False

    global_median = float(global_df["price"].median())
    winner_idx = global_df["price"].idxmin()
    winner_price = float(global_df.loc[winner_idx, "price"])
    winner_url = str(global_df.loc[winner_idx, "url"])
    winner_title = str(global_df.loc[winner_idx, "title"])
    winner_store = str(global_df.loc[winner_idx, "store"])
    winner_image_url = str(global_df.loc[winner_idx, "image_url"] or "").strip() or None

    winner_discount = 0.0
    if global_median > 0:
        winner_discount = (global_median - winner_price) / global_median

    processed = process_products(
        config,
        data_ws,
        [Product(name=winner_title, price=winner_price, url=winner_url, image_url=winner_image_url)],
        search_keyword,
        alert_chat_id=alert_chat_id,
    )

    if winner_discount >= 0.10:
        message = (
            f"🏆 Melhor oferta global para '{search_keyword}'!\n"
            f"Loja: {winner_store}\n"
            f"Produto: {winner_title}\n"
            f"Preço: R$ {brl(winner_price)}\n"
            f"Mediana global: R$ {brl(global_median)}\n"
            f"Desconto: {winner_discount * 100:.2f}%\n"
            f"Link: {winner_url}"
        )
        if config.telegram_enabled:
            enviar_alerta_telegram(
                chat_id=str(alert_chat_id or "").strip(),
                produto=winner_title,
                preco=winner_price,
                mediana=global_median,
                desconto=winner_discount,
                link=winner_url,
                image_url=winner_image_url,
            )
            logger.info("Alerta GLOBAL enviado para '%s'.", search_keyword)
        else:
            logger.info("Telegram desabilitado. Mensagem global gerada: %s", message)
    else:
        logger.info(
            "Desconto global insuficiente para '%s': %.2f%% (< 10%%).",
            search_keyword,
            winner_discount * 100,
        )

    return processed


def get_monitoring_targets(
    scraper: MultiMarketplaceScraper,
    target_ws,
    fallback_keywords: List[str],
) -> List[Dict[str, str]]:
    ensure_target_headers(target_ws)
    rows = target_ws.get_all_records()
    targets: List[Dict[str, str]] = []
    seen = set()

    for row in rows:
        chat_id = str(row.get("Chat_ID", "") or "").strip()
        term = str(row.get("Termo Buscado", "") or "").strip()
        blacklist = str(row.get("Blacklist", "") or "").strip()
        if not term:
            continue

        if not chat_id or chat_id.upper() == "GLOBAL":
            continue

        key = f"{chat_id}:{term.lower()}"
        if key in seen:
            continue
        seen.add(key)
        targets.append({"keyword": term, "chat_id": chat_id, "blacklist": blacklist})

    return targets


def main() -> int:
    try:
        config = load_config()
        scraper = MultiMarketplaceScraper(build_browser_context)
        sh = open_spreadsheet(config)
        data_ws = get_or_create_worksheet(sh, config.data_sheet_name)
        target_ws = get_or_create_worksheet(sh, config.target_sheet_name)
        baseline_ws = get_or_create_worksheet(
            sh,
            os.getenv("BASELINE_SHEET_NAME", "Baselines").strip() or "Baselines",
        )
        scraper_log_ws = get_or_create_worksheet(sh, config.scraper_log_sheet_name, rows=1000, cols=12)
        ensure_scraper_log_headers(scraper_log_ws)
        execution_id = datetime.now().strftime("%Y%m%d%H%M%S")

        processed_any_term = False
        monitoring_targets = get_monitoring_targets(
            scraper,
            target_ws,
            config.search_keywords,
        )

        if not monitoring_targets:
            logger.warning(
                "Nenhum alvo com Chat_ID válido em '%s'. Alerts serão enviados somente para quem cadastrou via Telegram.",
                config.target_sheet_name,
            )
            return 0

        for target in monitoring_targets:
            keyword = target["keyword"]
            chat_id = str(target.get("chat_id", "") or "").strip()
            scoped_blacklist_terms = parse_item_blacklist(str(target.get("blacklist", "") or ""))
            logger.info("Iniciando busca global para termo: %s", keyword)
            processed = daily_monitor(
                scraper=scraper,
                config=config,
                data_ws=data_ws,
                baseline_ws=baseline_ws,
                scraper_log_ws=scraper_log_ws,
                search_keyword=keyword,
                alert_chat_id=chat_id,
                item_blacklist_terms=scoped_blacklist_terms,
                execution_id=execution_id,
            )
            processed_any_term = processed_any_term or processed

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
