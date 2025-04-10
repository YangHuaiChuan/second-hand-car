import requests
from bs4 import BeautifulSoup
import pandas as pd
import random
import time
import traceback
from fake_useragent import UserAgent
import re
from config import Config
from concurrent.futures import ThreadPoolExecutor

# 省份字典
PROVINCES = {
    # "安徽": "https://www.che168.com/anhui/list/?pvareaid=100943",
    # "福建": "https://www.che168.com/fujian/list/?pvareaid=100943",
    # "广东": "https://www.che168.com/guangdong/list/?pvareaid=100943",
    # "广西": "https://www.che168.com/guangxi/list/?pvareaid=100943",
    # "贵州": "https://www.che168.com/guizhou/list/?pvareaid=100943",
    # "甘肃": "https://www.che168.com/gansu/list/?pvareaid=100943",
    # "海南": "https://www.che168.com/hainan/list/?pvareaid=100943",
    # "河南": "https://www.che168.com/henan/list/?pvareaid=100943",
    # "河北": "https://www.che168.com/hebei/list/?pvareaid=100943",
    # "湖北": "https://www.che168.com/hubei/list/?pvareaid=100943",
    # "湖南": "https://www.che168.com/hunan/list/?pvareaid=100943",
    # "吉林": "https://www.che168.com/jilin/list/?pvareaid=100943",
    # "江苏": "https://www.che168.com/jiangsu/list/?pvareaid=100943",
    # "江西": "https://www.che168.com/jiangxi/list/?pvareaid=100943",
    # "辽宁": "https://www.che168.com/liaoning/list/?pvareaid=100943",
    # "山东": "https://www.che168.com/shandong/list/?pvareaid=100943",
    "黑龙江": "https://www.che168.com/heilongjiang/list/?pvareaid=100943",
    "内蒙古": "https://www.che168.com/neimenggu/list/?pvareaid=100943",
    "宁夏": "https://www.che168.com/ningxia/list/?pvareaid=100943",
    "青海": "https://www.che168.com/qinghai/list/?pvareaid=100943",
    "上海": "https://www.che168.com/shanghai/list/?pvareaid=100943",
    "云南": "https://www.che168.com/yunnan/list/?pvareaid=100943",
    "重庆": "https://www.che168.com/chongqing/list/?pvareaid=100943",
    "新疆": "https://www.che168.com/xinjiang/list/?pvareaid=100943",
    "西藏": "https://www.che168.com/xizang/list/?pvareaid=100943",
    "山西": "https://www.che168.com/shanxi/list/?pvareaid=100943",
    "陕西": "https://www.che168.com/shan_xi/list/?pvareaid=100943",
    "北京": "https://www.che168.com/beijing/list/?pvareaid=100943",
    "四川": "https://www.che168.com/sichuan/list/?pvareaid=100943",
    "天津": "https://www.che168.com/tianjin/list/?pvareaid=100943",
    "浙江": "https://www.che168.com/zhejiang/list/?pvareaid=100943"
}

# 获取代理配置
def get_proxies():
    return {
        'http': f'http://{Config.proxy_username}:{Config.proxy_password}@{Config.proxy_address}:{Config.proxy_port}',
        'https': f'http://{Config.proxy_username}:{Config.proxy_password}@{Config.proxy_address}:{Config.proxy_port}',
    }

# 获取请求头
def get_headers():
    return {
        'User-Agent': UserAgent().random,
        'Referer': 'https://www.che168.com'
    }

# 获取单个页面的车辆链接
def get_car_links_from_page(base_url, page_num):
    headers = get_headers()
    proxies = get_proxies()
    car_links = []

    url = f"{base_url}&page={page_num}"
    try:
        res = requests.get(url, headers=headers, proxies=proxies, timeout=10)
        if res.status_code == 200:
            soup = BeautifulSoup(res.text, 'html.parser')
            a_tags = soup.select('.cards-li a')
            print(f"第 {page_num} 页找到 {len(a_tags)} 个链接")
            for a in a_tags:
                href = a.get('href')
                if href:
                    full_link = "https://www.che168.com" + href
                    car_links.append(full_link)
        else:
            print(f"第 {page_num} 页请求失败，状态码：", res.status_code)
    except Exception:
        print(f"第 {page_num} 页抓取出错:")
        traceback.print_exc()
    return car_links

# 获取所有车辆链接
def get_car_links_requests(base_url, max_pages=100):
    all_links = []
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(get_car_links_from_page, base_url, page) for page in range(1, max_pages + 1)]
        for future in futures:
            all_links.extend(future.result())
    return all_links

# 获取每辆车的详细信息
def parse_car_detail_with_requests(idx, url):
    headers = get_headers()
    proxies = get_proxies()

    try:
        response = requests.get(url, headers=headers, proxies=proxies, timeout=10)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')

            info_tag = soup.select_one('.car-box .car-brand-name') or \
                       soup.select_one('.car-box .car-brand-name .icon-cxc') or \
                       soup.select_one('.bread-crumbs .content a:last-of-type')

            if not info_tag:
                print(f"第 {idx} 辆车基本信息未找到")
                return None

            car_info = info_tag.text.strip()
            brand_style_model = car_info.split()
            brand = brand_style_model[0]
            style = brand_style_model[1] if len(brand_style_model) > 1 else ''
            model = " ".join(brand_style_model[2:]) if len(brand_style_model) > 2 else ''

            price_tag = soup.select_one('.car-box .brand-price-item .price') or \
                        soup.select_one('.cxc-priceCard .left-module .goodstartmoney')
            new_price_tag = soup.select_one('.car-box .brand-price-item .price-nom')

            if not price_tag:
                print(f"第 {idx} 辆车现价未找到")
                return None

            current_price = price_tag.get_text(strip=True).replace('万', '').replace('￥', '').replace('¥', '') + '万'

            new_car_price = ''
            if new_price_tag:
                new_car_price = new_price_tag.get_text(strip=True)
                new_car_price = re.split(r'[:：]', new_car_price)
                new_car_price = new_car_price[1].strip() if len(new_car_price) > 1 else new_car_price[0].strip()

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
                '现价': current_price,
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
            print(f"第 {idx} 辆车抓取到的信息: {car_data_info}")
            return car_data_info
        else:
            print(f"请求失败，第 {idx} 页，状态码：{response.status_code}")
    except Exception:
        print(f"第 {idx} 辆车请求异常:")
        traceback.print_exc()

# 主流程
def main():
    for province_name, base_url in PROVINCES.items():
        print(f"\n开始抓取 {province_name} 的车辆数据...")
        all_links = get_car_links_requests(base_url)
        print(f"\n{province_name} 共获取 {len(all_links)} 个详情页链接")

        cars_data = []
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = []
            for idx, link in enumerate(all_links, 1):
                print(f"\n正在抓取第 {idx} 辆车详情: {link}")
                futures.append(executor.submit(parse_car_detail_with_requests, idx, link))

            for future in futures:
                car_info = future.result()
                if car_info:
                    cars_data.append(car_info)
                else:
                    print("抓取失败，跳过")

        if cars_data:
            df = pd.DataFrame(cars_data)
            filename = f"二手车数据_汽车之家_{province_name}_Requests版.xlsx"
            df.to_excel(filename, index=False)
            print(f"{province_name} 数据保存成功，共抓取 {len(cars_data)} 条记录 -> {filename}")
        else:
            print(f"{province_name} 没有抓到任何车辆信息。")

if __name__ == '__main__':
    main()
