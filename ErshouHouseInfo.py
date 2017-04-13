#!/usr/bin/python3
#encoding:utf-8

import urllib.request
import urllib.error
from bs4 import BeautifulSoup
import re
import random
import time
import http.server
import http.client
import math
import os
import sys
import socket
import pymysql

#from LianJiaLogIn import AnaLogin

UserAgent_List = [
            "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/22.0.1207.1 Safari/537.1",
            "Mozilla/5.0 (X11; CrOS i686 2268.111.0) AppleWebKit/536.11 (KHTML, like Gecko) Chrome/20.0.1132.57 Safari/536.11",
            "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.6 (KHTML, like Gecko) Chrome/20.0.1092.0 Safari/536.6",
            "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.6 (KHTML, like Gecko) Chrome/20.0.1090.0 Safari/536.6",
            "Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/19.77.34.5 Safari/537.1",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/536.5 (KHTML, like Gecko) Chrome/19.0.1084.9 Safari/536.5",
            "Mozilla/5.0 (Windows NT 6.0) AppleWebKit/536.5 (KHTML, like Gecko) Chrome/19.0.1084.36 Safari/536.5",
            "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1063.0 Safari/536.3",
            "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1063.0 Safari/536.3",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_0) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1063.0 Safari/536.3",
            "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1062.0 Safari/536.3",
            "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1062.0 Safari/536.3",
            "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.1 Safari/536.3",
            "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.1 Safari/536.3",
            "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.1 Safari/536.3",
            "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.0 Safari/536.3",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/535.24 (KHTML, like Gecko) Chrome/19.0.1055.1 Safari/535.24",
            "Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/535.24 (KHTML, like Gecko) Chrome/19.0.1055.1 Safari/535.24"
    ]

Area_name = {'dongcheng':'东城', 'xicheng':'西城', 'chaoyang':'朝阳', 'haidian':'海淀',\
        'fengtai':'丰台', 'shijingshan':'石景山', 'tongzhou':'通州', 'changping':'昌平',\
        'daxing':'大兴', 'yizhuangkaifaqu':'亦庄开发区', 'shunyi':'顺义', 'fangshan':'房山',\
        'mentougou':'门头沟', 'pinggu':'平谷', 'huairou':'怀柔', 'miyun':'密云',\
        'yanqing':'延庆', 'yanjiao':'燕郊'}

# 测试IP是否可用的的网站
URL_test='https://www.baidu.com/'
# 准备采集多少个IP备用
num_IP = 2
# 测试IP时超过多少秒不响应就舍弃了
IP_test_timeout = 1.5
# 改变ip代理的次数,用于确定header
change_ip_times = 0;

## --------------------------------------------------------------------------

# 测试IP地址是否可用,时间为1秒
def IP_Test(IP,URL_test,set_timeout=IP_test_timeout):
    #print("IP_test")
    try:
        proxy_handler = urllib.request.ProxyHandler({'http':IP})
        opener = urllib.request.build_opener(proxy_handler)
        urllib.request.install_opener(opener)

        req = urllib.request.Request(URL_test)
        conn = urllib.request.urlopen(req, timeout = IP_test_timeout)
        return True
    except:
        return False

## --------------------------------------------------------------------------

# 获取可用的IP地址
def get_IP_list():
    #print("get_IP_list")
    total_IP_num = 0
    test_Url = 'http://www.baidu.com'
    IP_list = []
    for i in range(1, 20):
        url_page = 'http://www.xicidaili.com/nn/' + str(i)
        req = urllib.request.Request(url_page, headers = get_header())
        response = urllib.request.urlopen(req)
        html = response.read().decode('utf-8')
        p = re.compile(r'''<tr\sclass[^>]*>\s+<td[^>]*>\s*.*\s*</td>\s+<td>(.*)?</td>\s+<td>(.*)?</td>\s+<td>\s*.*\s*</td>\s+<td[^>]*>(.*)?</td>\s+<td>(.*)?</td>''')
        proxy_list = p.findall(html)
        for each_proxy in proxy_list:
            if each_proxy[2] == "高匿" and  each_proxy[3] == 'HTTP':
                proxy = each_proxy[0] + ':' + each_proxy[1]
                if IP_Test(proxy, test_Url) == True:
                    IP_list.append(proxy)
                    total_IP_num += 1
                    if total_IP_num == num_IP:
                        return IP_list
    return IP_list

##--------------------------------------------------------------------------------

# 随机获取一个IP
def get_random_IP():
    #print("get_random_IP")
    IP_list = get_IP_list()
    if IP_list != []:
        ind = random.randint(0, len(IP_list)-1)
        return IP_list[ind]
    return None

# 获取随机的header
def get_header():
    #print("get_header")
    global change_ip_times
    return {'User-Agent': UserAgent_List[change_ip_times % len(UserAgent_List)],\
            'Accept': "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",\
            'Cache-Control': 'no-cache',\
            'Upgrade-Insecure-Requests': '1',\
            }

enctype = 'utf-8'
max_error_times = 5

##----------------------------------------------------------------------------

# 获取网页内容
def get_page(url):
    #print("get_page")
    error_time = 0
    while True:
        try:
            if error_time == max_error_times:
                print('失败次数达到%d次' % max_error_times)
                return None
            error_time += 1
            req = urllib.request.Request(url, headers=get_header())
            html = urllib.request.urlopen(req)
            return html
        except (urllib.error.URLError, urllib.error.HTTPError) as e:
            if hasattr(e, 'code'):
                print(e.code, e.reason)
                print("Error1")
                change_proxy()
                continue
            elif hasattr(e, 'reason'):
                print("Error2")
                print(e)
                change_proxy()
                continue
        except (ConnectionResetError, http.client.BadStatusLine) as e:
            print(e)
            print("Error3")
            change_proxy()
            continue
        except socket.TimeoutError as e:
            print(e)
            print('服务器长时间没有响应')
            change_proxy()
            continue
        else:
            print("Error4")

##---------------------------------------------------------------------------------

# 使用代理IP
def change_proxy():
    #print("change_proxy")
    global change_ip_times
    change_ip_times += 1
    proxy = get_random_IP()
    if proxy == None:
        proxy_support = urllib.request.ProxyHandler({})
    else:
        proxy_support = urllib.request.ProxyHandler({'http':proxy})
    opener = urllib.request.build_opener(proxy_support)
    opener.addheaders = [('User-Agent', UserAgent_List[change_ip_times])]
    urllib.request.install_opener(opener)
    print('智能切换代理:%s' % ('本机' if proxy==None else proxy))

##-----------------------------------------------------------------------------------

# 获取不同城区房源的总数
def getTotalNum(url):
    #print("getTotalNum")
    # 打开网页，获取网页内容
    try:
        html = get_page(url)
        #html = urllib.request.urlopen(req)
    except (urllib.error.HTTPError, urllib.error.URLError) as e:
        return None

    # 获取房源总数
    try:
        bsObj = BeautifulSoup(html.read(), 'html5lib')
        totalNum = bsObj.find("h2", {"class":"total fl"}).find('span')
        totalNum = totalNum.get_text()
    except AttributeError as e:
        print("AttributeError1")
        print('1')
        return None

    return totalNum

##--------------------------------------------------------------------------

# 获取房源详细信息
def getHouseInfo(url, table_name):
    #print("getHouseInfo")
    # 打开网页，获取网页内容
    try:
        html = get_page(url)
        if html== None:
            print("未获取信息!")
    except (urllib.error.HTTPError, urllib.error.URLError) as e:
        return None


    # 连接数据库
    conn = pymysql.connect(host='127.0.0.1', unix_socket='/run/mysqld/mysqld.sock',\
              user='root', passwd='mysql密码', db = 'BeiJing', charset = "utf8")
    cur = conn.cursor()
    cur.execute("use BeiJing;")
    conn.commit()

    # 获取房源详细信息
    try:
        bsObj = BeautifulSoup(html.read(), 'html5lib')
        HouseInfoList = bsObj.findAll("div", {"class":"info clear"})

        for HouseInfo in HouseInfoList:
            info_dict = []

            # Obj = BeautifulSoup(html.read(), 'html5lib')''))取小区名称及链接
            housetitle = HouseInfo.find("div", {"class":"title"})
            info_dict.append(housetitle.a.get('href'))

            # 获取房源信息(名称,户型,面积,朝向)
            houseaddr = HouseInfo.find("div", {"class":"address"})
            info = houseaddr.div.get_text().split("|")
            info_dict.append('北京' + Area_name[table_name] + ' ' + info[0])
            info_dict.append(info[1])
            info_dict.append(info[2])
            info_dict.append(info[3])

            # 获取房源楼层及年份
            housefloor = HouseInfo.find("div", {"class":"flood"})
            floor_all = housefloor.div.get_text().split('-')[0].strip().split(' ')
            floorInfo = floor_all[0].split(')')
            info_dict.append(floorInfo[0]+')')
            info_dict.append(floorInfo[-1])

            # 获取房屋总价及均价
            totalPrice = HouseInfo.find("div", {"class":"totalPrice"})
            info_dict.append(totalPrice.span.get_text())

            unitPrice = HouseInfo.find("div", {"class":"unitPrice"})
            info_dict.append(unitPrice.get("data-price"))

            sql = 'insert into ' + table_name + ' (Href, CellName, HouseStyle,\
                Square, Direction, Floor, Year, TotalPrice, UnitPrice) values \
                (%s, %s, %s, %s, %s, %s, %s, %s, %s);'
            cur.execute(sql, info_dict)
            conn.commit()
        cur.close()
        conn.close()
    except AttributeError as e:
        print("AttributeError2")
        return None

    return info_dict

##-------------------------------------------------------------------------

# 获取城区内不同划片的房源链接
def getAreaURL(url, URLDict):
    #print("getAreaURL")
    # 打开网页，获取网页内容
    try:
        req = urllib.request.Request(url, headers=get_header())
        html = urllib.request.urlopen(req)
    except (urllib.error.HTTPError, urllib.error.URLError) as e:
        print('Error1')
        return None

    AreaURL = []
    try:
        bsObj = BeautifulSoup(html.read(), 'html5lib')
        URLink =  bsObj.find('div', {"data-role":"ershoufang"})
        for link in URLink.findAll("a", href=re.compile("/ershoufang/*")):
            if link.attrs['href'] not in URLDict:
                AreaURL.append(link.attrs['href'])
    except:
        return None

    return AreaURL

##-------------------------------------------------------------------------

# 获取不同城区房源的链接
def getLinkURL(url):
    #print("getLinkURL")
    # 打开网页，获取网页内容
    try:
        req = urllib.request.Request(url, headers=get_header())
        html = urllib.request.urlopen(req)
    except (urllib.error.HTTPError, urllib.error.URLError) as e:
        print('Error1')
        return None

    # 获取不同城区网页链接
    linkURL = []
    try:
        bsObj = BeautifulSoup(html.read(), 'html5lib')
        for link in bsObj.findAll("a", href=re.compile("^/ershoufang/.+"), title=re.compile("^(北京)")):
            linkURL.append(link.attrs['href'])
    except AttributeError as e:
        print("attributeError3")
        return None

    return linkURL

##-----------------------------------------------------------------------

if __name__ == '__main__':

    # 首先模拟登录
    #LoginResult = AnaLogin()
    print("yes!!")
    # 获取不同城区房源的网页链接
    result = getLinkURL("http://bj.lianjia.com/ershoufang/")
    print(result)
    if result == None:
        print ("URL is not available!!")

    while True:
        if result == []:
            change_proxy()
            result = getLinkURL("http://bj.lianjia.com/ershoufang/")
        elif result == None:
            change_proxy()
            result = getLinkURL("http://bj.lianjia.com/ershoufang/")
        else:
            break

   # 获取不同城区房源总数
    for url in result:
        # 输出房源信息
        name = url.split('/')
        # 连接数据库,并判断表是否为空，若不为空跳过
        conn = pymysql.connect(host='127.0.0.1', unix_socket='/run/mysqld/mysqld.sock',\
              user='root', passwd='mysql密码', db = 'BeiJing', charset= "utf8")
        cur = conn.cursor()
        cur.execute("use BeiJing;")
        conn.commit()
        sql = 'select * from '+ (name[-2]) + ';'
        cur.execute(sql)
        empty = cur.fetchall()
        if empty:
           continue
        cur.close()
        conn.close()

        DistLink = 'http://bj.lianjia.com' + url
        HouseNum = getTotalNum(DistLink)
        HouseTotalNum = int(HouseNum)
        PageNum = HouseTotalNum / 30
        if (HouseTotalNum % 30) != 0:
            PageNum = PageNum + 1
        # 遍历每一页网页
        for i in range(1, int(PageNum)+1):
            newPage = DistLink + 'pg' + str(i) + '/'
            info = getHouseInfo(newPage, name[-2])
            time.sleep(random.randint(10, 15))
        time.sleep(3600)
    exit()
