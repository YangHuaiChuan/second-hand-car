import requests
from bs4 import BeautifulSoup
import pandas as pd
import random
import time
from fake_useragent import UserAgent
import re
import traceback
from config import Config

# 获取列表页中所有车辆的链接
def get_car_links_requests():
    headers = {
        'User-Agent': UserAgent().random,
        'Referer': 'https://www.che168.com'
    }

    car_links = []
    base_url = 'https://www.che168.com/china/list/?pvareaid=100945'

    for page_num in range(1, 4):  # 抓取前 3 页
        print(f"\n正在抓取第 {page_num} 页...")
        url = f'{base_url}&page={page_num}'
        try:
            proxies = {
                'http': f'http://{Config.proxy_username}:{Config.proxy_password}@{Config.proxy_address}:{Config.proxy_port}',
                'https': f'http://{Config.proxy_username}:{Config.proxy_password}@{Config.proxy_address}:{Config.proxy_port}',
            }
            res = requests.get(url, headers=headers,proxies=proxies,timeout=10)
            if res.status_code == 200:
                soup = BeautifulSoup(res.text, 'html.parser')
                a_tags = soup.select('.cards-li a')
                print(f"找到 {len(a_tags)} 个链接")
                for a in a_tags:
                    href = "https://www.che168.com/" + a.get('href')
                    if href:
                        car_links.append(href)
            else:
                print("页面请求失败，状态码：", res.status_code)
        except Exception as e:
            print("抓取出错:")
            traceback.print_exc()  # 打印详细的错误信息，包括堆栈跟踪
        time.sleep(random.uniform(1, 3))  # 防封
    return car_links

# 获取每一辆车的详细信息
def parse_car_detail_with_requests(idx, url):
    headers = {
        'User-Agent': UserAgent().random,
        'Referer': 'https://www.che168.com'
    }

    try:
        proxies = {
            'http': f'http://{Config.proxy_username}:{Config.proxy_password}@{Config.proxy_address}:{Config.proxy_port}',
            'https': f'http://{Config.proxy_username}:{Config.proxy_password}@{Config.proxy_address}:{Config.proxy_port}',
        }
        response = requests.get(url, headers=headers,proxies=proxies,timeout=10)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')

            # 获取车辆品牌、型号、款式等信息
            info_tag = soup.select_one('.car-box .car-brand-name')

            # 如果没有找到，再尝试获取 .car-box .car-brand-name .icon-cxc
            if not info_tag:
                info_tag = soup.select_one('.car-box .car-brand-name .icon-cxc')

            # 从面包屑获取基本信息
            if not info_tag:
                info_tag = soup.select_one('.bread-crumbs .content a:last-of-type')

            if not info_tag:
                print(f"第 {idx} 辆车基本信息未找到")
                return None

            car_info = info_tag.text.strip()
            brand_style_model = car_info.split()
            brand = brand_style_model[0]
            style = brand_style_model[1] if len(brand_style_model) > 1 else ''
            model = " ".join(brand_style_model[2:]) if len(brand_style_model) > 2 else ''

            # 提取价格信息
            price_tag = soup.select_one('.car-box .brand-price-item .price')
            new_price_tag = soup.select_one('.car-box .brand-price-item .price-nom')

            if not price_tag:
                price_tag = soup.select_one('.cxc-priceCard .left-module .goodstartmoney')

            if not price_tag:
                print(f"第 {idx} 辆车现价未找到")
                return None

            current_price = ''
            new_car_price = ''

            if price_tag:
                current_price = price_tag.get_text(strip=True).replace('万', '').replace('￥', '').replace('¥', '')
                current_price += '万'  # 恢复单位

            if new_price_tag:
                new_car_price = new_price_tag.get_text(strip=True)
                # 使用正则表达式匹配英文冒号或中文冒号进行分割
                new_car_price = re.split(r'[:：]', new_car_price)
                new_car_price = new_car_price[1].strip() if len(new_car_price) > 1 else new_car_price[0].strip()
            


            # 获取详细信息（遍历 li 而不是 ul）
            car_profile = {}
            li_items = soup.select('.all-basic-content .basic-item-ul li')

            for li in li_items:
                key_el = li.select_one('.item-name')
                if key_el:
                    key = key_el.get_text(strip=True).replace('\u00a0', '').replace('：', '').replace(' ', '')
                    value = li.get_text(strip=True).replace(key_el.get_text(strip=True), '').strip()
                    if key and value:
                        car_profile[key] = value

            car_data_info = {
                '品牌': brand,
                '款式': style,
                '型号': model,
                '现价':current_price,
                '新车价格': new_car_price,
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

            return car_data_info
        else:
            print(f"请求失败，第 {idx} 页，状态码：{response.status_code}")
    except Exception as e:
        print(f"第 {idx} 辆车请求异常:")
        traceback.print_exc()  # 打印详细的错误信息，包括堆栈跟踪


# 主程序
if __name__ == '__main__':
    all_links = get_car_links_requests()
    print(f"\n共获取 {len(all_links)} 个详情页链接")

    cars_data = []
    for idx, link in enumerate(all_links, 1):
        print(f"\n正在抓取第 {idx} 辆车详情: {link}")
        car_info = parse_car_detail_with_requests(idx, link)
        if car_info:
            print("抓取成功:", car_info)
            cars_data.append(car_info)
        else:
            print("抓取失败，跳过")
        time.sleep(random.uniform(1.5, 3))  # 限速防封

    if cars_data:
        df = pd.DataFrame(cars_data)
        df.to_excel("二手车数据_汽车之家_Requests版.xlsx", index=False)
        print(f"\n共抓取 {len(cars_data)} 辆车信息，保存成功！")
    else:
        print("没有抓到任何车辆信息。")

