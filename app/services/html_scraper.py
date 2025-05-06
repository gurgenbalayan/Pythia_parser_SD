import asyncio
import random

from fake_useragent import UserAgent
from bs4 import BeautifulSoup
from selenium.webdriver import Keys, ActionChains
from selenium.webdriver.support.wait import WebDriverWait
from selenium import webdriver
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from utils.logger import setup_logger
import os
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

SELENIUM_REMOTE_URL = os.getenv("SELENIUM_REMOTE_URL")
STATE = os.getenv("STATE")
logger = setup_logger("scraper")

def clean_url_and_extract_type(url: str):
    parsed = urlparse(url)
    query_dict = parse_qs(parsed.query)
    biz_type = query_dict.pop('type', [None])[0]  # Извлекаем и удаляем 'type'

    # Пересобираем query string без 'type'
    cleaned_query = urlencode(query_dict, doseq=True)
    cleaned_url = urlunparse(parsed._replace(query=cleaned_query))

    return cleaned_url, biz_type
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
async def fetch_company_details(url: str) -> dict:
    driver = None
    referer_url = "https://sosenterprise.sd.gov/BusinessServices/Business/FilingSearch.aspx"
    try:
        ua = UserAgent()
        user_agent = ua.random
        options = webdriver.ChromeOptions()
        options.add_argument(f'--user-agent={user_agent}')
        options.add_argument(f'--lang=en-US')
        options.add_argument("--start-maximized")
        options.add_argument("--disable-webrtc")
        options.add_argument("--disable-features=WebRtcHideLocalIpsWithMdns")
        options.add_argument("--force-webrtc-ip-handling-policy=default_public_interface_only")
        options.add_argument("--disable-features=DnsOverHttps")
        options.add_argument("--no-default-browser-check")
        options.add_argument("--no-first-run")
        options.add_argument("--no-sandbox")
        options.add_argument("--test-type")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option("useAutomationExtension", False)
        options.set_capability("goog:loggingPrefs", {
            "performance": "ALL",
            "browser": "ALL"
        })
        driver = webdriver.Remote(
            command_executor=SELENIUM_REMOTE_URL,
            options=options
        )
        driver.set_page_load_timeout(30)
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
        return {}
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
    try:
        ua = UserAgent()
        user_agent = ua.random
        url = "https://sosenterprise.sd.gov/BusinessServices/Business/FilingSearch.aspx"
        options = webdriver.ChromeOptions()
        options.add_argument(f'--user-agent={user_agent}')
        options.add_argument(f'--lang=en-US')
        options.add_argument("--start-maximized")
        options.add_argument("--disable-webrtc")
        options.add_argument("--disable-features=WebRtcHideLocalIpsWithMdns")
        options.add_argument("--force-webrtc-ip-handling-policy=default_public_interface_only")
        options.add_argument("--disable-features=DnsOverHttps")
        options.add_argument("--no-default-browser-check")
        options.add_argument("--no-first-run")
        options.add_argument("--no-sandbox")
        options.add_argument("--test-type")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option("useAutomationExtension", False)
        options.set_capability("goog:loggingPrefs", {
            "performance": "ALL",
            "browser": "ALL"
        })
        options.page_load_strategy = 'eager'
        driver = webdriver.Remote(
            command_executor=SELENIUM_REMOTE_URL,
            options=options
        )
        driver.set_page_load_timeout(30)
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
