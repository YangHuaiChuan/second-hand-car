from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from fake_useragent import UserAgent
import random
import time
import pandas as pd
from base64 import b64encode
from config import Config  # 假设你有一个 config.py 文件，里面定义了 Config 类

# 获取站大爷的代理配置，包括用户名和密码
def get_random_proxy():
    proxy_list = [
        f'{Config.proxy_address}:{Config.proxy_port}'  # 示例: '123.45.67.89:1080'
    ]
    return random.choice(proxy_list)

# 设置代理认证
def get_proxy_authentication_header(username, password):
    # 将用户名和密码进行 base64 编码
    credentials = f'{username}:{password}'
    b64_credentials = b64encode(credentials.encode('utf-8')).decode('utf-8')
    return f'Basic {b64_credentials}'

def start_browser_with_proxy(proxy_ip, proxy_username, proxy_password):
    chrome_options = Options()
    chrome_options.add_argument('--headless')  # 使浏览器在后台运行
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument(f'--proxy-server=http://{proxy_ip}')  # 配置代理
    
    # 设置代理认证
    auth_header = get_proxy_authentication_header(proxy_username, proxy_password)
    chrome_options.add_argument(f'--proxy-auth={auth_header}')  # 添加认证头

    chrome_options.add_argument(f"user-agent={UserAgent().random}")  # 设置随机的 User-Agent
    service = Service(r'D:\UserApps\ChromeDriver\chromedriver.exe')
    return webdriver.Chrome(service=service, options=chrome_options)

# 获取车辆列表链接
def get_car_links_from_che168():
    proxy = get_random_proxy()
    driver = start_browser_with_proxy(proxy, Config.proxy_username, Config.proxy_password)
    print(f"使用代理：{proxy}")

    url = 'https://www.che168.com/china/list/?pvareaid=100945'
    driver.get(url)

    car_links = []
    page_num = 1

    while True:
        try:
            print(f"\n正在抓取第 {page_num} 页链接...")
            WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, '.tp-cards-tofu .cards-li a')))
            time.sleep(random.uniform(2, 5))  # 防止被封
            # 获取当前页面的 HTML
            html_content = driver.page_source
            array_a = driver.find_elements(By.CSS_SELECTOR, '.tp-cards-tofu .cards-li a')

            print(f"找到 {len(array_a)} 个链接")

            for a in array_a:
                href = a.get_attribute('href')
                if href:
                    car_links.append(href)

            if page_num >= 3:
                break

            # 翻页逻辑
            try:
                next_button = driver.find_element(By.CSS_SELECTOR, '.page .page-item-next')
                if 'disabled' in next_button.get_attribute('class'):
                    print("已到最后一页")
                    break
                next_button.click()
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, '.tp-cards-tofu .cards-li'))
                )
                page_num += 1
            except Exception as e:
                print("翻页失败:", e)
                break

        except Exception as e:
            print("页面加载失败:", e)
            driver.save_screenshot(f"error_page_{page_num}.png")
            break

    driver.quit()
    return car_links

# 获取每辆车的详细信息
def parse_car_detail_with_driver(idx, url):
    proxy = get_random_proxy()
    driver = start_browser_with_proxy(proxy, Config.proxy_username, Config.proxy_password)
    try:
        driver.get(url)
        WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.CSS_SELECTOR, '.car-box .car-brand-name')))

        car_info = driver.find_element(By.CSS_SELECTOR, '.car-box .car-brand-name').text
        print(f"第 {idx} 辆车抓取到的信息: {car_info}")

        brand_style_model = car_info.split()
        brand = brand_style_model[0]  # 品牌
        style = brand_style_model[1]  # 款式
        model = " ".join(brand_style_model[2:])

        # 获取汽车的详细信息
        items = driver.find_elements(By.CSS_SELECTOR, '.all-basic-content .basic-item-ul')
        car_profile = {}
        for li in items:
            try:
                key_el = li.find_element(By.CSS_SELECTOR, '.item-name')
                key_text = key_el.text.strip().replace('\u00a0', '').replace('：', '')
                key_clean = ''.join(key_text.split())  # 清理多余空格

                full_text = li.text.strip()
                value = full_text.replace(key_el.text, '').strip()

                if key_clean and value:
                    car_profile[key_clean] = value
            except:
                continue

        car_data_info = {
            '品牌': brand,
            '款式': style,
            '型号': model,
            '燃料类型': '汽油' if car_profile.get('燃油标号') else '电动',
            '上牌时间': car_profile.get('上牌时间', ''),
            '表显里程': car_profile.get('表显里程', ''),
            '变速箱': car_profile.get('变速箱', ''),
            '排量': car_profile.get('排量', ''),
            '发布时间': car_profile.get('发布时间', ''),
            '年检到期': car_profile.get('年检到期', ''),
            '保险到期': car_profile.get('保险到期', ''),
            '过户次数': car_profile.get('过户次数', ''),
            '所在地': car_profile.get('所在地', ''),
            '发动机': car_profile.get('发动机', ''),
            '车辆级别': car_profile.get('车辆级别', ''),
            '车身颜色': car_profile.get('车身颜色', ''),
            '驱动方式': car_profile.get('驱动方式', '')
        }

        print(f"第 {idx} 辆车抓取到的详细信息: {car_data_info}")
        return car_data_info
    except Exception as e:
        print("Selenium 抓取失败:", e)
    finally:
        driver.quit()
    return None

# 主程序
if __name__ == '__main__':
    all_links = get_car_links_from_che168()
    print(f"\n共获取 {len(all_links)} 个详情页链接")

    cars_data = []
    for idx, link in enumerate(all_links, 1):
        print(f"\n正在抓取第 {idx} 辆车详情: {link}")
        car_info = parse_car_detail_with_driver(idx, link)  # 或 parse_car_detail_with_requests(link)
        if car_info:
            print("抓取成功:", car_info)
            cars_data.append(car_info)
        else:
            print("抓取失败，跳过")

    if cars_data:
        df = pd.DataFrame(cars_data)
        df.to_excel("二手车数据_汽车之家.xlsx", index=False)
        print(f"\n共抓取 {len(cars_data)} 辆车信息，保存成功！")
    else:
        print("没有抓到任何车辆信息。")
