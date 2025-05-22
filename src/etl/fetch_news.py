from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
import time
import pandas as pd
from datetime import datetime, timedelta

# cutoff time
now = datetime.now()
cutoff = now.replace(hour=0, minute=0, second=0, microsecond=0)

# browser config
options = Options()
options.add_argument("--headless")
options.add_argument("--start-maximized")
driver = webdriver.Chrome(options=options)
actions = ActionChains(driver)

# URL
url = "https://mktnews.com/index.html"
driver.get(url)
WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, "body")))

# chat deletion
try:
    driver.execute_script("""
        const target = Array.from(document.querySelectorAll('div')).find(el =>
            el.getAttribute('style') === 'position: static; top: var(--header-height); right: 0px; height: fit-content;'
        );
        if (target) target.remove();
    """)
    time.sleep(1)
except Exception as e:
    print(f"⚠️ Nie udało się usunąć wskazanego div'a: {e}")


# header finder
header = driver.find_element(By.ID, "flash_header")

#get value
size = header.size
width = size['width']
height = size['height']
actions = ActionChains(driver)
actions.move_to_element_with_offset(header, width // 2, height // 2).click().perform()

# scroll by pg dn
SCROLL_PAUSE = 2
MAX_SCROLLS = 50
MAX_STUCK_SCROLLS = 30
scroll_count = 0
stuck_scrolls = 0

body = driver.find_element(By.TAG_NAME, "body")
previous_items_count = 0

# datalist
data = []

while scroll_count < MAX_SCROLLS:
    scroll_count += 1
    print(f"\n Page Down {scroll_count}/{MAX_SCROLLS}")

    body.send_keys(Keys.PAGE_DOWN)
    time.sleep(SCROLL_PAUSE)

    try:   # load more button finder 
        load_more_button = WebDriverWait(driver, 3).until(
            EC.element_to_be_clickable((By.CLASS_NAME, "button-loadmore--more"))
        )
        driver.execute_script("arguments[0].scrollIntoView();", load_more_button)
        time.sleep(1)
        driver.execute_script("arguments[0].click();", load_more_button)
        time.sleep(SCROLL_PAUSE)
    except Exception:
        pass

    news_items = driver.find_elements(By.CLASS_NAME, "flash-item")
    current_items_count = len(news_items)

    # stuck checker
    if current_items_count == previous_items_count:
        stuck_scrolls += 1
        if stuck_scrolls >= MAX_STUCK_SCROLLS:
            break
    else:
        stuck_scrolls = 0
    previous_items_count = current_items_count
        #only new data
    for item in news_items[len(data):]:
        try:
            time_elem = item.find_elements(By.TAG_NAME, "span")
            raw_time = time_elem[0].text.strip() if time_elem else None

            content_elem = item.find_elements(By.CLASS_NAME, "flash-content")
            content = content_elem[0].text.strip() if content_elem else None
            #data parsing
            if raw_time and content:
                news_time = datetime.combine(now.date(), datetime.strptime(raw_time, "%H:%M:%S").time())
                if news_time > now:
                    news_time -= timedelta(days=1)
            #cutoff
                if news_time >= cutoff:
                    data.append({
                        "date": news_time.strftime("%Y-%m-%d %H:%M:%S"),
                        "content": content
                    })
                else:
                    print("Cutoff time found.")
                    scroll_count = MAX_SCROLLS
                    break
        except Exception as e:
            print(f"Parsing Error {e}")

# CSV write
df = pd.DataFrame(data)
df.to_csv("data/raw/mkt_news.csv", index=False)
print(f"Found {len(data)} elements.")
print("Data saved into data/raw/mkt_news.csv")


driver.quit()
