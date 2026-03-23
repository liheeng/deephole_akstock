import random
import time
import requests
from urllib.parse import urlparse

# =========================
# UA（进程级固定）
# =========================
UA_LIST = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",

    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_3) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",

    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
]

SESSION_UA = random.choice(UA_LIST)

# =========================
# 通用头
# =========================
COMMON_HEADERS = {
    "User-Agent": SESSION_UA,
    "Accept": "*/*",
    "Accept-Language": "zh-CN,zh;q=0.9",
    "Connection": "keep-alive",
}

# =========================
# 数据源 Header（精细到子域名）
# =========================
HOST_HEADERS = {
    # ===== 新浪 =====
    "finance.sina.com.cn": {
        "Referer": "https://finance.sina.com.cn",
    },
    "quotes.sina.cn": {
        "Referer": "https://quotes.sina.cn",
    },

    # ===== 东方财富 =====
    "push2.eastmoney.com": {
        "Referer": "https://quote.eastmoney.com",
    },
    "push2his.eastmoney.com": {
        "Referer": "https://quote.eastmoney.com",
    },
    "quote.eastmoney.com": {
        "Referer": "https://quote.eastmoney.com",
    },

    # ===== 网易 =====
    "api.money.163.com": {
        "Referer": "https://money.163.com",
    },

    # ===== 腾讯 =====
    "qt.gtimg.cn": {
        "Referer": "https://stock.qq.com",
    },

    # ===== 同花顺 =====
    "data.10jqka.com.cn": {
        "Referer": "http://data.10jqka.com.cn",
    },

    # ===== 深交所 =====
    "www.szse.cn": {
        "Referer": "https://www.szse.cn/",
        "Origin": "https://www.szse.cn",
    },

    # ===== 上交所 =====
    "query.sse.com.cn": {
        "Referer": "https://www.sse.com.cn/",
        "Origin": "https://www.sse.com.cn",
    },
}

# =========================
# Host匹配
# =========================
def match_host_headers(host: str):
    # 精确匹配
    if host in HOST_HEADERS:
        return HOST_HEADERS[host]

    # 模糊匹配（兜底）
    for key in HOST_HEADERS:
        if key in host:
            return HOST_HEADERS[key]

    return {}


# =========================
# 特殊增强逻辑
# =========================
def enhance_headers(host: str, headers: dict):
    # 上交所（SSE）
    if "sse.com.cn" in host:
        headers.update({
            "Host": "query.sse.com.cn",
            "Pragma": "no-cache",
            "Cache-Control": "no-cache",
        })

    # 深交所（SZSE）
    if "szse.cn" in host:
        headers["X-Requested-With"] = "XMLHttpRequest"

    return headers


# =========================
# 构造 headers
# =========================
def build_headers(url: str, custom_headers: dict | None = None):
    parsed = urlparse(url)
    host = parsed.netloc

    headers = COMMON_HEADERS.copy()

    # 1️⃣ 按 host 匹配
    headers.update(match_host_headers(host))

    # 2️⃣ 特殊增强
    headers = enhance_headers(host, headers)

    # 3️⃣ 用户自定义优先
    if custom_headers:
        headers.update(custom_headers)

    return headers


# =========================
# Patch requests
# =========================
_original_request = requests.sessions.Session.request


def patched_request(self, method, url, **kwargs):
    # 👉 随机延迟（防封关键）
    # time.sleep(random.uniform(0.3, 1.2))
    time.sleep(random.uniform(0.2, 0.8))

    custom_headers = kwargs.get("headers", {})
    headers = build_headers(url, custom_headers)
    kwargs["headers"] = headers

    # 👉 自动重试
    retries = 3
    for i in range(retries):
        try:
            return _original_request(self, method, url, **kwargs)
        except Exception as e:
            if i == retries - 1:
                raise
            time.sleep(1 + i)


def patch_requests():
    if requests.sessions.Session.request != patched_request:
        requests.sessions.Session.request = patched_request