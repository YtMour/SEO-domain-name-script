import csv
import requests
import time
import os
import json
import signal
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter, Retry
import random
import threading

# 全局停止标志
stop_all = False
cache_lock = threading.Lock()  # 缓存写锁
file_lock = threading.Lock()   # 文件写锁

def signal_handler(sig, frame):
    global stop_all
    print("\n[!] 检测到 Ctrl+C，正在安全退出……")
    stop_all = True

signal.signal(signal.SIGINT, signal_handler)

BLACKLIST_KEYWORDS = [
    "sex", "porn", "casino", "bet", "gamble", "xxx", "escort",
    "poker", "adult", "lottery", "hentai", "baccarat"
]
# 取消后缀限制
# ACCEPTED_TLDS = ["com", "net", "org"]

MIN_BACKLINKS = 50
MIN_SNAPSHOTS = 5
MAX_RETRY = 3

def is_top_domain(info):
    return info['Backlinks'] > 3500 and info['Snapshots'] > 20 and info['ACR'] > 50.0

INPUT_FILE = "domains.csv"
OUTPUT_FILE = "filtered_domains.csv"
OUTPUT_TOP_FILE = "top_domains.csv"
OUTPUT_TEXT_FILE = "filtered_domains.txt"
FAILED_RETRY_FILE = "failed_retry_domains.json"
CACHE_FILE = "wayback_cache.json"

MAX_WORKERS = 10  # 线程数根据机器调节

PROXY_PORTS = range(30001, 30852)
PROXIES_LIST = [
    {
        "http": f"http://127.0.0.1:{port}",
        "https": f"http://127.0.0.1:{port}"
    }
    for port in PROXY_PORTS
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/137.0.0.0 Safari/537.36"
}

session = requests.Session()
retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
session.mount('https://', HTTPAdapter(max_retries=retries))
session.headers.update(HEADERS)

def get_random_proxy():
    if PROXIES_LIST:
        return random.choice(PROXIES_LIST)
    else:
        return None

def is_safe_domain(domain):
    return not any(kw in domain.lower() for kw in BLACKLIST_KEYWORDS)

def load_cache(filename):
    if os.path.exists(filename):
        with open(filename, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {}

def save_cache(cache, filename):
    with cache_lock:
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(cache, f, ensure_ascii=False, indent=2)

def load_passed_domains():
    passed = set()
    if os.path.exists(OUTPUT_FILE):
        with open(OUTPUT_FILE, 'r', encoding='utf-8') as f:
            next(f)  # 跳过表头
            for line in f:
                domain = line.split(',')[0].strip().lower()
                passed.add(domain)
    return passed

def load_failed_retry_domains():
    if os.path.exists(FAILED_RETRY_FILE):
        with open(FAILED_RETRY_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {}

def save_failed_retry_domains(data):
    with open(FAILED_RETRY_FILE, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

# 反链数解析函数，支持带单位如 "29.9 K", "1.2M"
def parse_backlinks(value):
    if not value:
        return 0
    value = str(value).strip().lower().replace(',', '')
    multiplier = 1
    if value.endswith('k'):
        multiplier = 1_000
        value = value[:-1]
    elif value.endswith('m'):
        multiplier = 1_000_000
        value = value[:-1]
    elif value.endswith('b'):
        multiplier = 1_000_000_000
        value = value[:-1]
    try:
        return int(float(value) * multiplier)
    except:
        return 0

# Wayback获取快照数量
def query_wayback(domain, timeout=5):
    url = f"https://web.archive.org/cdx/search/cdx?url={domain}&output=json"
    try:
        resp = session.get(url, proxies=get_random_proxy(), timeout=timeout)
        if resp.status_code == 200:
            data = resp.json()
            return max(len(data) - 1, 0)  # 第一行为字段名
    except Exception as e:
        print(f"[Wayback] 快照请求错误 {domain} -> {e}")
    return None

def query_memento(domain, timeout=5):
    url = f"http://timetravel.mementoweb.org/api/json/{domain}"
    try:
        resp = session.get(url, proxies=get_random_proxy(), timeout=timeout)
        if resp.status_code == 200:
            data = resp.json()
            if 'mementos' in data and 'list' in data['mementos']:
                return len(data['mementos']['list'])
    except Exception as e:
        print(f"[Memento] 快照请求错误 {domain} -> {e}")
    return None

def query_archive_today(domain, timeout=5):
    url = f"https://archive.ph/{domain}"
    try:
        resp = session.get(url, proxies=get_random_proxy(), timeout=timeout)
        if resp.status_code == 200 and "Wayback Machine" not in resp.text:
            return 1
    except Exception as e:
        print(f"[Archive.today] 快照请求错误 {domain} -> {e}")
    return None

def get_wayback_snapshots(domain):
    for func in [query_wayback, query_memento, query_archive_today]:
        count = func(domain)
        if count is not None:
            return count
    return -1

# 新增：判断是否一级域名，排除多级
def is_valid_domain(domain):
    domain = domain.lower().strip()
    if domain.count('.') != 1:
        return False, "非一级域名"
    return True, None

def process_domain(row, cache, passed_domains):
    global stop_all
    if stop_all:
        return None, "用户请求终止"

    domain = row.get('domain') or row.get('Domain')
    if not domain:
        return None, "域名缺失"

    domain_lc = domain.lower()

    # 判断是否一级域名
    valid, reason = is_valid_domain(domain_lc)
    if not valid:
        return domain_lc, reason

    if domain_lc in passed_domains:
        return domain_lc, "之前已合格，跳过"

    # 不再判断后缀是否接受，已在 is_valid_domain 判断过

    # 反链数用新的解析函数
    backlinks_raw = row.get('backlinks') or row.get('Backlinks') or row.get('bl') or 0
    backlinks = parse_backlinks(backlinks_raw)

    try:
        acr = float(row.get('acr') or row.get('ACR') or 0)
    except:
        acr = 0.0

    try:
        wby = int(row.get('wby') or row.get('WBY') or 0)
    except:
        wby = 0

    try:
        aby = int(row.get('aby') or row.get('ABY') or 0)
    except:
        aby = 0

    if not is_safe_domain(domain):
        return domain_lc, "包含敏感词"
    if backlinks < MIN_BACKLINKS:
        return domain_lc, f"反链数低({backlinks}<{MIN_BACKLINKS})"

    with cache_lock:
        snapshots = cache.get(domain_lc)
    if snapshots is None:
        snapshots = get_wayback_snapshots(domain_lc)
        with cache_lock:
            cache[domain_lc] = snapshots

    if snapshots == -1:
        return domain_lc, "快照请求失败"
    if snapshots < MIN_SNAPSHOTS:
        return domain_lc, f"快照数低({snapshots}<{MIN_SNAPSHOTS})"

    info = {
        'Domain': domain_lc,
        'Backlinks': backlinks,
        'Snapshots': snapshots,
        'ACR': acr,
        'WBY': wby,
        'ABY': aby,
        'Top': is_top_domain({
            'Backlinks': backlinks,
            'Snapshots': snapshots,
            'ACR': acr
        })
    }
    return info, None

def extract_top_from_filtered():
    if not os.path.exists(OUTPUT_FILE):
        print("❌ 找不到已筛选文件。")
        return

    TOP_TEXT_FILE = "top_domains_sorted.txt"
    domains = []

    with open(OUTPUT_FILE, 'r', encoding='utf-8') as infile:
        reader = csv.DictReader(infile)
        for row in reader:
            try:
                backlinks = int(row['Backlinks'])
                snapshots = int(row['Snapshots'])
                acr = float(row['ACR'])

                if is_top_domain({'Backlinks': backlinks, 'Snapshots': snapshots, 'ACR': acr}):
                    domains.append({
                        'Domain': row['Domain'],
                        'Backlinks': backlinks,
                        'Snapshots': snapshots,
                        'ACR': acr
                    })
            except Exception:
                continue

    # 自定义排序权重（你可以根据需求调整权重比例）
    def score(d):
        return d['Backlinks'] * 0.5 + d['Snapshots'] * 0.3 + d['ACR'] * 0.2

    # 按综合得分降序排列
    domains.sort(key=score, reverse=True)

    with open(TOP_TEXT_FILE, 'w', encoding='utf-8') as outfile:
        for d in domains:
            line = f"{d['Domain']} | 反链: {d['Backlinks']} | 快照: {d['Snapshots']} | ACR: {d['ACR']}\n"
            outfile.write(line)

    print(f"✅ 共提取极品域名 {len(domains)} 条，已按综合评分排序并保存到 {TOP_TEXT_FILE}")

def filter_domains():
    global stop_all

    if not os.path.exists(INPUT_FILE):
        print(f"❌ 未找到输入文件：{INPUT_FILE}")
        return

    passed_domains = load_passed_domains()
    failed_retry_domains = load_failed_retry_domains()
    snapshot_cache = load_cache(CACHE_FILE)

    input_domains = {}
    with open(INPUT_FILE, newline='', encoding='utf-8') as infile:
        reader = csv.DictReader(infile)
        for row in reader:
            domain = (row.get('domain') or row.get('Domain') or '').lower()
            if not domain or domain in passed_domains or domain in failed_retry_domains:
                continue
            input_domains[domain] = {"row": row, "retry_count": 0}

    # 合并失败重试列表
    for domain, data in failed_retry_domains.items():
        input_domains[domain] = data

    with open(OUTPUT_FILE, 'a', newline='', encoding='utf-8') as outfile, \
         open(OUTPUT_TEXT_FILE, 'a', encoding='utf-8') as textfile:

        fieldnames = ['Domain', 'Backlinks', 'Snapshots', 'ACR', 'WBY', 'ABY']
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        if os.stat(OUTPUT_FILE).st_size == 0:
            writer.writeheader()

        failed_next_retry = {}

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {}
            for domain, data in input_domains.items():
                futures[executor.submit(process_domain, data['row'], snapshot_cache, passed_domains)] = (domain, data.get('retry_count', 0))

            try:
                for future in as_completed(futures):
                    if stop_all:
                        print("[!] 用户请求终止，停止处理剩余任务。")
                        break

                    domain, retry_count = futures.pop(future)
                    try:
                        info, error = future.result()
                    except Exception as e:
                        print(f"[!] 任务异常: {domain} -> {e}")
                        if retry_count + 1 < MAX_RETRY:
                            failed_next_retry[domain] = {"row": input_domains[domain]['row'], "retry_count": retry_count + 1}
                        else:
                            print(f"[!] 域名 {domain} 达到最大重试次数，放弃。")
                        continue

                    if error:
                        if error == "之前已合格，跳过":
                            print(f"⏭ 跳过: {domain} | {error}")
                            failed_next_retry.pop(domain, None)
                        elif error == "用户请求终止":
                            print(f"[!] 用户请求终止，跳过后续。")
                            stop_all = True
                            break
                        elif error == "快照请求失败":
                            if retry_count + 1 < MAX_RETRY:
                                print(f"🟡 不合格: {domain} | {error}，正在第 {retry_count+1} 次重新请求……")
                                failed_next_retry[domain] = {"row": input_domains[domain]['row'], "retry_count": retry_count + 1}
                            else:
                                print(f"❌ 不合格: {domain} | 原因: 达到最大重试次数，放弃")
                        else:
                            print(f"❌ 不合格: {domain} | 原因: {error}")
                    else:
                        with file_lock:
                            print(f"✅ 合格: {domain} | 反链: {info['Backlinks']} | 快照: {info['Snapshots']}")
                            writer.writerow({k: info[k] for k in fieldnames})
                            textfile.write(f"{domain} | 反链: {info['Backlinks']} | 快照: {info['Snapshots']} | ACR: {info['ACR']}\n")
                        failed_next_retry.pop(domain, None)

                    save_cache(snapshot_cache, CACHE_FILE)

            except KeyboardInterrupt:
                stop_all = True
                print("\n[!] 捕获到中断，安全退出。")

        save_cache(snapshot_cache, CACHE_FILE)
        save_failed_retry_domains(failed_next_retry)

    print("\n✅ 筛选完成！合格域名已保存到", OUTPUT_FILE)
    if failed_next_retry:
        print(f"⚠️ 失败的域名已保存到 {FAILED_RETRY_FILE}，下次运行时会自动重试。")
    else:
        print("🎉 所有域名处理完成，无需重试。")

if __name__ == "__main__":
    print("请选择模式：\n1 - 筛选域名（从原始 CSV 中读取，输出筛选结果）\n2 - 从已筛选域名中提取极品")
    mode = input("输入选项 (1/2)：").strip()
    if mode == "1":
        filter_domains()
    elif mode == "2":
        extract_top_from_filtered()
    else:
        print("无效选项，退出。")
