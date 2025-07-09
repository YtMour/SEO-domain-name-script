from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import time
import csv
import os

TARGET_URL = "https://member.expireddomains.net/domains/combinedexpired/?savedsearch_id=546720&fabirth_year=2024&fwhoisage=2025&fbl=500&facr=35&fadult=1&fwhois=22&o=domainpop&r=d"

options = webdriver.ChromeOptions()
# options.add_argument("--headless")  # 需要时开启无头模式
options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                     "AppleWebKit/537.36 (KHTML, like Gecko) "
                     "Chrome/114.0.0.0 Safari/537.36")

driver = webdriver.Chrome(options=options)
wait = WebDriverWait(driver, 20)

def wait_for_manual_login():
    driver.get("https://expireddomains.net")
    print("请手动登录账号，登录完成后访问目标页面：")
    print(TARGET_URL)
    input("登录并打开目标页面后，按回车继续爬取...")

def parse_table():
    try:
        wait.until(EC.presence_of_element_located((By.XPATH, '//*[@id="listing"]/table')))
    except TimeoutException:
        print("表格加载超时")
        return []

    table = driver.find_element(By.XPATH, '//*[@id="listing"]/table')
    headers = table.find_elements(By.XPATH, './/thead/tr/th')

    header_map = {}
    for idx, th in enumerate(headers):
        text = th.text.strip().lower()
        if text == 'domain':
            header_map['domain'] = idx
        elif text == 'bl':
            header_map['bl'] = idx
        elif text == 'wby':
            header_map['wby'] = idx
        elif text == 'aby':
            header_map['aby'] = idx
        elif text == 'acr':
            header_map['acr'] = idx

    rows = table.find_elements(By.XPATH, './/tbody/tr')
    data = []

    for row in rows:
        cols = row.find_elements(By.TAG_NAME, "td")
        try:
            domain = cols[header_map['domain']].text.strip() if 'domain' in header_map else ""
            bl = cols[header_map['bl']].text.strip() if 'bl' in header_map else "0"
            wby = cols[header_map['wby']].text.strip() if 'wby' in header_map else "0"
            aby = cols[header_map['aby']].text.strip() if 'aby' in header_map else "0"
            acr = cols[header_map['acr']].text.strip() if 'acr' in header_map else "0"

            data.append({
                "domain": domain,
                "bl": bl,
                "wby": wby,
                "aby": aby,
                "acr": acr,
            })
        except Exception as e:
            print(f"单行数据解析异常，跳过此行: {e}")
            continue
    return data

def go_next_page():
    try:
        next_btn = driver.find_element(By.XPATH, '//*[@id="listing"]/div[2]/div[2]/div[1]/a')
        if "disabled" in next_btn.get_attribute("class") or next_btn.get_attribute("aria-disabled") == "true":
            return False
        current_first = driver.find_element(By.XPATH, '//*[@id="listing"]/table/tbody/tr[1]/td[1]').text
        next_btn.click()
        wait.until(lambda d: d.find_element(By.XPATH, '//*[@id="listing"]/table/tbody/tr[1]/td[1]').text != current_first)
        return True
    except Exception as e:
        print(f"翻页失败或无下一页: {e}")
        return False

def load_existing_domains(filename):
    existing = set()
    if os.path.exists(filename):
        with open(filename, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                domain = row.get("domain", "").strip().lower()
                if domain:
                    existing.add(domain)
    return existing

def main():
    wait_for_manual_login()

    all_data = []
    page_num = 1
    MAX_PAGES = 100

    while True:
        print(f"正在爬取第 {page_num} 页")
        page_data = parse_table()
        if not page_data:
            print("本页无数据，停止爬取")
            break
        all_data.extend(page_data)

        if MAX_PAGES and page_num >= MAX_PAGES:
            print(f"达到最大页数限制 {MAX_PAGES}，停止爬取")
            break

        if not go_next_page():
            print("没有下一页，爬取结束")
            break
        page_num += 1

    print(f"共爬取 {len(all_data)} 条记录")

    existing_domains = load_existing_domains("domains.csv")

    # 过滤重复域名
    new_data = [d for d in all_data if d["domain"].lower() not in existing_domains]

    if not new_data:
        print("没有新域名数据，跳过写入文件。")
    else:
        file_exists = os.path.exists("domains.csv")
        with open("domains.csv", "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["domain", "bl", "wby", "aby", "acr"])
            # 如果文件不存在或空，写表头
            if not file_exists or os.stat("domains.csv").st_size == 0:
                writer.writeheader()
            writer.writerows(new_data)
        print(f"追加写入了 {len(new_data)} 条新记录到 domains.csv")

    driver.quit()
    print("浏览器关闭，程序结束。")

if __name__ == "__main__":
    main()
