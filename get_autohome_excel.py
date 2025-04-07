from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import pandas as pd
import random
from fake_useragent import UserAgent

def get_random_proxy():
    proxy_list = [
        '39.107.249.241:3883'
    ]
    return random.choice(proxy_list)

def start_browser_with_proxy(proxy_ip):
    chrome_options = Options()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument(f'--proxy-server=http://{proxy_ip}')
    chrome_options.add_argument(f"user-agent={UserAgent().random}")
    service = Service(r'D:\MySoftware\ChromeDriver\chromedriver.exe')
    return webdriver.Chrome(service=service, options=chrome_options)

def scrape_che168():
    proxy = get_random_proxy()
    driver = start_browser_with_proxy(proxy)
    print(f"使用代理：{proxy}")

    url = 'https://www.che168.com/nlist/shenzhen/list/?pvareaid=100533'
    driver.get(url)

    cars_data = []

    page_num = 1
    while True:
        try:
            print(f"\n正在抓取第 {page_num} 页...")
            WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, '.tp-cards-tofu .cards-li a')))
            time.sleep(random.uniform(3, 6))  # 防止被封

            # 重新获取车辆链接列表
            array_a = driver.find_elements(By.CSS_SELECTOR, '.tp-cards-tofu .cards-li a')
            print(f"找到 {len(array_a)} 辆车")

            for item_a in array_a:
                try:
                    detail_url = item_a.get_attribute('href')
                    # print(f"抓取的第{len(cars_data)+1}车辆详情链接: {detail_url}")

                    # 在此处跳转到详情页
                    driver.get(detail_url)
                    WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.CSS_SELECTOR, '.car-box .car-brand-name')))

                    # 提取品牌、款式、型号信息
                    car_info = driver.find_element(By.CSS_SELECTOR, '.car-box .car-brand-name').text
                    print(f"第{len(cars_data)+1}辆车信息: {car_info}")

                    # 解析品牌、款式、型号
                    try:
                        # 例如：宝马X3(进口) 2011款 xDrive28i 豪华型
                        brand_style_model = car_info.split()
                        brand = brand_style_model[0]
                        style = brand_style_model[1]
                        model = " ".join(brand_style_model[2:])

                        print(f"品牌: {brand}, 款式: {style}, 型号: {model}")

                        # 保存车辆数据
                        cars_data.append({
                            '品牌': brand,
                            '款式': style,
                            '型号': model
                        })

                    except Exception as e:
                        print(f"第{len(cars_data)+1}辆车的信息提取失败:", e)
                        continue
                        
                except Exception as e:
                    print("跳过一个异常项:", e)
                    continue

            if page_num >= 3:  # 限制抓取前3页
                break

            # 检查是否有“下一页”按钮
            try:
                next_button = driver.find_element(By.CSS_SELECTOR, '.page .page-item-next')
                if 'disabled' in next_button.get_attribute('class'):
                    print("已到最后一页。")
                    break
                else:
                    next_button.click()
                    page_num += 1
            except Exception as e:
                print("未找到下一页按钮或点击失败:", e)
                break

        except Exception as e:
            print("页面加载失败:", e)
            driver.save_screenshot("debug_screenshot.png")
            break
    driver.quit()

    if cars_data:
        df = pd.DataFrame(cars_data)
        df.to_excel('二手车数据_汽车之家.xlsx', index=False)
        print(f"\n共抓取 {len(cars_data)} 辆车，数据已保存为 Excel 文件！")
    else:
        print("没有抓取到任何车辆信息。")


scrape_che168()