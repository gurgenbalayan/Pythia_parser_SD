import asyncio
import json
import random
import shutil
import sqlite3
import string
import time

from fake_useragent import UserAgent
from bs4 import BeautifulSoup
from selenium.webdriver import Keys, ActionChains
from selenium.webdriver.support.wait import WebDriverWait
from selenium import webdriver
import undetected_chromedriver as uc
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from utils.logger import setup_logger
import os
from dotenv import load_dotenv
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse


def clean_url_and_extract_type(url: str):
    parsed = urlparse(url)
    query_dict = parse_qs(parsed.query)
    biz_type = query_dict.pop('type', [None])[0]  # Извлекаем и удаляем 'type'

    # Пересобираем query string без 'type'
    cleaned_query = urlencode(query_dict, doseq=True)
    cleaned_url = urlunparse(parsed._replace(query=cleaned_query))

    return cleaned_url, biz_type
load_dotenv()

STATE = os.getenv("STATE")
logger = setup_logger("scraper")

def get_cookie_file() -> str:
    script_dir = os.path.dirname(os.path.realpath(__file__))
    cookie_dir = os.path.join(script_dir,"cookies")
    txt_files = [f for f in os.listdir(cookie_dir) if f.endswith(".txt")]
    if not txt_files:
        raise FileNotFoundError("No .txt files found in the specified folder.")
    random_file = random.choice(txt_files)
    return os.path.join(cookie_dir, random_file)

def get_chrome_timestamp():
    """
    Возвращает текущее время в формате WebKit timestamp (микросекунды с 1601 года).
    """
    CHROME_EPOCH_DIFF = 11644473600  # Секунд между 1601 и 1970
    now = time.time()  # Текущее время в секундах
    return int((now + CHROME_EPOCH_DIFF) * 1_000_000)
def load_cookies(db_path, cookies_file2, cookie_file):
    try:
        with open(cookie_file, "r") as f:
            cookies = json.load(f)
        script_dir = os.path.dirname(os.path.realpath(__file__))
        db_path = os.path.join(script_dir, db_path)
        db_path2 = os.path.join(script_dir, cookies_file2)
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        conn2 = sqlite3.connect(db_path2)
        cursor2 = conn2.cursor()
        values_list = []
        for cookie in cookies:
            now_chrome = get_chrome_timestamp()
            expires_chrome = now_chrome + (30 * 24 * 60 * 60 * 1_000_000)  # +30 дней
            # Значения для вставки
            values = (
                now_chrome,  # creation_utc
                cookie["domain"],  # host_key
                '',  # top_frame_site_key (можно оставить пустым)
                cookie["name"],  # name
                cookie["value"],  # value
                b'',  # encrypted_value
                cookie.get("path", "/"),  # path
                expires_chrome,  # expires_utc
                int(cookie.get("secure", False)),  # is_secure
                int(cookie.get("httpOnly", False)),  # is_httponly
                now_chrome,  # last_access_utc
                1,  # has_expires
                1,  # is_persistent
                1,  # priority (обычно 1)
                0,  # samesite (None = 0, Lax = 1, Strict = 2)
                2,  # source_scheme (Unset = 0, NonSecure = 1, Secure = 2)
                443,  # source_port (обычный HTTP порт)
                now_chrome,  # last_update_utc
                2,  # source_type
                1  # has_cross_site_ancestor
            )
            values_list.append(values)
            # SQL-запрос
        sql = """
                INSERT INTO cookies (
                    creation_utc,
                    host_key,
                    top_frame_site_key,
                    name,
                    value,
                    encrypted_value,
                    path,
                    expires_utc,
                    is_secure,
                    is_httponly,
                    last_access_utc,
                    has_expires,
                    is_persistent,
                    priority,
                    samesite,
                    source_scheme,
                    source_port,
                    last_update_utc,
                    source_type,
                    has_cross_site_ancestor
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """

        cursor.executemany(sql, values_list)
        conn.commit()
        cursor2.executemany(sql, values_list)
        conn2.commit()
        conn2.close()
        conn.close()
        print("✅ Cookie inserted.")
        return True
    except Exception as e:
        print("! Cookies not inserted")
        print(e)
        return False
async def human_typing_with_mouse(driver, element, text: str, delay_range=(0.1, 0.3)):
    """Наводит мышку, кликает, потом вводит текст по-человечески."""
    try:
        # Навести мышку и кликнуть
        actions = ActionChains(driver)
        actions.move_to_element(element).pause(random.uniform(0.3, 0.8)).click().perform()

        await asyncio.sleep(random.uniform(0.3, 0.6))  # небольшая пауза после клика

        # Ввод по буквам
        for char in text:
            element.send_keys(char)
            await asyncio.sleep(random.uniform(*delay_range))
        element.send_keys(Keys.RETURN)
    except Exception as e:
        print(f"Ошибка при вводе текста: {e}")
async def generate_random_user_agent():
    browsers = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Firefox/89.0',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/91.0.864.48',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_6) AppleWebKit/537.36 (KHTML, like Gecko) Firefox/89.0',
    ]
    return random.choice(browsers)
async def fetch_company_details(url: str) -> dict:
    driver = None
    referer_url = "https://sosenterprise.sd.gov/BusinessServices/Business/FilingSearch.aspx"
    try:
        ua = UserAgent()
        user_agent = ua.random
        options = webdriver.ChromeOptions()
        options.add_argument(f'--user-agent={user_agent}')
        options.add_argument('--lang=en-US')
        options.add_argument("--headless=new")
        options.add_argument("--start-maximized")
        options.page_load_strategy = 'eager'
        options.add_argument("--disable-webrtc")
        options.add_argument("--disable-features=WebRtcHideLocalIpsWithMdns")
        options.add_argument("--force-webrtc-ip-handling-policy=default_public_interface_only")
        options.add_argument("--window-size=1920,1080")
        options.add_argument("--disable-features=DnsOverHttps")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-blink-features=AutomationControlled")
        driver = uc.Chrome(options=options)
        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
            "source": """
                        const getContext = HTMLCanvasElement.prototype.getContext;
                        HTMLCanvasElement.prototype.getContext = function(type, attrs) {
                            const ctx = getContext.apply(this, arguments);
                            if (type === '2d') {
                                const originalToDataURL = this.toDataURL;
                                this.toDataURL = function() {
                                    return "data:image/png;base64,fake_canvas_fingerprint";
                                };
                            }
                            return ctx;
                        };
                        """
        })
        driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
            'source': '''
                    Object.defineProperty(navigator, 'webdriver', {
                      get: () => undefined
                    })
                  '''
        })
        driver.get(referer_url)
        cleaned_url, biz_type = clean_url_and_extract_type(url)
        driver.execute_script(f"window.location.href='{cleaned_url}';")
        wait = WebDriverWait(driver, 15)  # Ожидаем до 15 секунд
        wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR,
                                                     "#ctl00_MainContent_updatePanel > div.container-fluid")))
        table = driver.find_element(By.CSS_SELECTOR, '#ctl00_MainContent_updatePanel > div.container-fluid')
        html = table.get_attribute('outerHTML')
        return await parse_html_details(html, biz_type)
    except Exception as e:
        logger.error(f"Error fetching data for url '{url}': {e}")
        return []
    finally:
        if driver:
            driver.quit()
def is_nothing_found_selenium(driver) -> bool:
    try:
        cell = driver.find_element(By.CSS_SELECTOR, "#DataTables_Table_0 > tbody > tr > td")
        return "no records found" in cell.text.strip().lower()
    except Exception:
        return False
async def fetch_company_data(query: str) -> list[dict]:
    driver = None
    driver2 = None
    try:
        ua = UserAgent()
        user_agent = ua.random
        url = "https://sosenterprise.sd.gov/BusinessServices/Business/FilingSearch.aspx"
        options = webdriver.ChromeOptions()
        options2 = webdriver.ChromeOptions()
        script_dir = os.path.dirname(os.path.realpath(__file__))
        profile = os.path.join(script_dir, "profile")
        if not os.path.exists(profile):
            os.makedirs(profile)
        else:
            shutil.rmtree(profile)
        word = ''.join(random.choices(string.ascii_letters, k=10))
        profile_path = os.path.join(profile, word)
        options.add_argument(f"--user-data-dir={profile_path}")
        options2.add_argument(f"--user-data-dir={profile_path}")
        options.add_argument(f'--user-agent={user_agent}')
        options.add_argument(f'--lang=en-US')
        options.add_argument("--headless=new")
        options2.add_argument("--headless=new")
        options.add_argument("--start-maximized")
        options.add_argument("--disable-webrtc")
        options.add_argument("--disable-features=WebRtcHideLocalIpsWithMdns")
        options.add_argument("--force-webrtc-ip-handling-policy=default_public_interface_only")
        options.add_argument("--window-size=1920,1080")
        options.add_argument("--disable-features=DnsOverHttps")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.page_load_strategy = 'eager'
        driver2 = uc.Chrome(options=options2)
        driver2.quit()
        driver2 = None
        cookies_file2 = os.path.join(profile_path, "Default", "Safe Browsing Cookies")
        cookies_file = os.path.join(profile_path, "Default", "Cookies")
        cookie_file = get_cookie_file()
        res_load = load_cookies(cookies_file, cookies_file2, cookie_file)
        if not res_load:
            print("Cookies failed to load!")
            return None, "", ""
        driver = uc.Chrome(options=options)
        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
            "source": """
                        const getContext = HTMLCanvasElement.prototype.getContext;
                        HTMLCanvasElement.prototype.getContext = function(type, attrs) {
                            const ctx = getContext.apply(this, arguments);
                            if (type === '2d') {
                                const originalToDataURL = this.toDataURL;
                                this.toDataURL = function() {
                                    return "data:image/png;base64,fake_canvas_fingerprint";
                                };
                            }
                            return ctx;
                        };
                        """
        })
        driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
            'source': '''
                    Object.defineProperty(navigator, 'webdriver', {
                      get: () => undefined
                    })
                  '''
        })
        driver.get(url)
        input_field = WebDriverWait(driver, 15).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "#ctl00_MainContent_txtSearchValue")))
        await human_typing_with_mouse(driver, input_field, query)

        wait = WebDriverWait(driver, 15)
        wait.until(EC.visibility_of_element_located(
            (By.CSS_SELECTOR, "#DataTables_Table_0 > thead > tr")))
        table = driver.find_element(By.CSS_SELECTOR,'#DataTables_Table_0 > tbody')
        html = table.get_attribute('outerHTML')
        return await parse_html_search(html)
    except Exception as e:
        nothing = is_nothing_found_selenium(driver)
        if nothing:
            return []
        logger.error(f"Error fetching data for query '{query}': {e}")
        return []
    finally:
        if driver2:
            driver.quit()
        if driver:
            driver.quit()


async def parse_html_search(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    results = []
    rows = soup.select("tbody > tr")
    for row in rows:
        try:
            link_tag = row.select_one("td.sorting_1 > a")
            tds = row.find_all("td")
            type = None
            if len(tds) > 1:
                type = tds[1].text.strip()
            biz_id = link_tag.text.strip()
            href = link_tag["href"]
            full_url = f"https://sosenterprise.sd.gov/BusinessServices/Business/{href}"
            name_td = row.find_all("td")[2]
            name_text = name_td.get_text(separator="\n").split("\n")[0].strip()
            status = row.find_all("td")[5].text.strip()
            results.append({
                "state": STATE,
                "name": name_text,
                "status": status,
                "id": biz_id,
                "url": full_url+f"&type={type}",
            })
        except Exception as e:
            logger.error(f"Error parse html search: {e}")
            continue
    return results


async def parse_html_details(html: str, biz_type: str) -> dict:
    soup = BeautifulSoup(html, "html.parser")

    def get_text(selector):
        el = soup.select_one(selector)
        return el.get_text(strip=True) if el else ""

    def get_inner_html(selector):
        el = soup.select_one(selector)
        return el.decode_contents().strip() if el else ""

    details = {
        "state": STATE,
        "name": get_text("#ctl00_MainContent_txtName"),
        "registration_number": get_text("#ctl00_MainContent_txtBusinessID"),
        "entity_type": biz_type,
        "status": get_text("#ctl00_MainContent_txtStatus"),
        "date_registered": get_text("#ctl00_MainContent_txtInitialDate"),
        "principal_address": get_inner_html("#ctl00_MainContent_txtOfficeAddresss"),
        "mailing_address": get_inner_html("#ctl00_MainContent_txtMailAddress"),
        "agent_name": get_text("#ctl00_MainContent_txtAgentName"),
        "agent_address": get_inner_html("#ctl00_MainContent_txtAgentAddress"),
        "agent_mailing_address": get_inner_html("#ctl00_MainContent_txtAgentAddressMail"),
        "documents": []
    }

    # Ищем документы
    history_section = soup.select("#ctl00_MainContent_divHistorySummary tr")
    for row in history_section:
        cols = row.find_all("td")
        if len(cols) >= 3:
            title = cols[0].get_text(strip=True)
            date = cols[1].get_text(strip=True)
            link_tag = cols[2].find("a")
            if link_tag and link_tag.get("href"):
                link = link_tag["href"]
                details["documents"].append({
                    "name": title,
                    "date": date,
                    "link": "https://sosenterprise.sd.gov/BusinessServices/Business/"+link
                })

    return details
