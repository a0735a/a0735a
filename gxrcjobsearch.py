#!/usr/bin/env python
# -*- encoding: utf-8 -*-
import threading

from selenium import webdriver
import requests
import time


import datetime





import asyncio
import os
import time
import aiofiles
import aiohttp
import aiosqlite3
import json
import logging
from aiohttp import web
import random
from urllib.parse import urlencode, urlparse, parse_qs

career1 = ''
career2 = ''
career3 = ''

class gxrc:
    dicWorkPlace = {'2', '3', '4', '10', '12', '13',
                    '69', '70', '71', '72', '74', '75', '77', '78', '150'}  # 字典：期望工作地
    dicEdu = {351, 352, 353, 354, 355,
              356, 357, 358, 359, 360}  # 字典：学历
    dicAge = {'0', '20', '21', '22', '23',
              '24', '25', '26', '27', '28', '29', '30', '31', '32', '33', '34', '35', '36', '37', '38', '39', '40'}  # 字典：年龄
    dicSex = {0, 1}  # 性别
    dicTalentDegree = {1, 10, 2, 3, 8}  # 人才类型

    condition_certain_urls = []  # 条件确定的并且结果数小于5000的url列表
    sql_params_list = []
    clientsession = None
    finish_url_count = 0
    proxy = None

    headers = {
        'Content-Type': 'application/json',
        'Accept-Encoding': 'gzip, deflate, br'
    }



    postdata_temp = {
        "SchoolID": "",
        "ageMax": 0,
        "ageMin": 0,
        "bussId": "0",
        "computerLevel": "",
        "desc": "desc",
        "domicile": 0,
        "drivingLicense": "",
        "edu": "",
        "eduBest": False,
        "englishLevel": "",
        "expectCareer": "",
        "industry": "",
        "isPhoto": False,
        "keyword": "",
        "lastLoginDate": 3,
        "num": 15,
        "orderBy": "",
        "page": 1,
        "pageSize": 15,
        "residency": "",
        "rid": "",
        "salaryMax": "",
        "salaryMin": "",
        "schField": "1",
        "schType": "",
        "sex": -1,
        "sortField": "",
        "specialty": "",
        "talentDegree": 0,
        "tallMax": 0,
        "tallMin": 0,
        "workPlace": "",
        "workTitle": "",
        "workYear": "",
        "workingState": ""
    }

    async def getCookies(self):
        # 设置浏览器默认存储地址
        options = webdriver.ChromeOptions()
        # options.add_argument('--headless')
        driver = webdriver.Chrome(options=options)
        driver.maximize_window()
        driver.get("https://vip.gxrc.com/gl/login")
        # 输入用户名
        driver.find_element_by_xpath('//*[@id="pane-login"]/div/form/div[1]/div/div/input').send_keys("lz121263")
        # 输入密码
        driver.find_element_by_xpath('//*[@id="pane-login"]/div/form/div[2]/div/div/input').send_keys("a11223344")

        # 等待拖拽验证码
        time.sleep(15)
        # 获取cookie
        cookies = driver.get_cookies()

        driver.close()
        return cookies


    async def insert_to_sqlite(self, loop, semaphore, data_list):
        # async with semaphore:
        async with aiosqlite3.connect('gxrc_resume_refresh_0930.db', check_same_thread=False, loop=loop) as conn:
            async with conn.cursor() as cursor:
                sql = 'INSERT OR IGNORE INTO gxrc_resume_refresh(resumeId,resumeGuid,lastLoginTime,age,salary,career1,career2,career3,residency) values (?,?,?,?,?,?,?,?,?)'
                try:
                    result = await cursor.executemany(sql, data_list)
                    await conn.commit()
                    return f'插入数据库成功'
                except Exception as e:
                    await conn.rollback()
                    print(f'insert_to_sqlite出错啦：{e}')
                    return f'插入数据库失败：'


    async def get_useable_proxy(self, session):
        # 检测代理是否可用
        if self.proxy == None:
            self.proxy = await self.get_random_proxy()
        is_proxy_useable = False
        for i in range(10):  # 试10次
            print(f'正在检测代理{proxy}……')
            try:
                async with session.get("https://vip.gxrc.com", proxy=self.proxy) as resp_test:
                    if resp_test.status == 200:
                        print(f'success:代理{proxy}可用')
                        is_proxy_useable = True
                        self.proxy = proxy
                        break
                    else:
                        print(f'正在请求新的代理……')
                        self.proxy = await self.get_random_proxy()
            except Exception as e:
                print(e)
                proxy = await self.get_random_proxy()
        return proxy


    async def get_random_proxy(self):
        async with aiohttp.ClientSession() as session:
            async with session.get('http://localhost:5555/random') as resp:
                result = await resp.text()
                proxy = f"http://{result}"
                print(proxy)
                return proxy


    async def aiodownload(self, loop, semaphore, url, cookies, session, is_for_product_url):
        async with semaphore:
            now = datetime.datetime.now()
            print(f"--------------aiodownload()--------{now}----------------{self.finish_url_count}/{len(self.condition_certain_urls)}--")
            query = urlparse(url).query
            params = parse_qs(query, keep_blank_values=True)
            if 'talentDegree=0' in url and 'sex=1' in url:
                print('fuck')
            dic = {}
            for key in params:
                value = params[key][0]
                dic[key] = value
                if isinstance(value, str):  # 如果是字符串
                    if value.isdigit():  # 如果是整数
                        dic[key] = int(value)
                    elif value == 'False':
                        dic[key] = False










            try:
                async with session.post(url.replace('False', 'false'), headers=self.headers, json=dic,
                                        proxy=self.proxy) as resp:  # resp = requests.get()
                    print(resp.status)
                    if resp.status == 401:  # 未登录
                        print(resp.status)
                    else:
                        result = await resp.json()
                        page = result['data']['page']
                        pagesize = result['data']['pageSize']
                        total = result['data']['total']
                        max_page = total / pagesize
                        datalist = result['data']['data']

                        for item in datalist:
                            degreeName = item['degreeName']
                            age = item['age']
                            expectedWorkPlaceName = item['expectedWorkPlaceName']
                            lastLoginTime = item['lastLoginTime']
                            resumeGuid = item['resumeGuid']
                            resumeId = item['resumeId']
                            expectSalary = item['expectSalary']
                            age = item['age']
                            text_Career = item['text_Career']

                            names = globals()
                            for i in range(len(text_Career)):
                                # 得到career1,career2,career3
                                names['career' +
                                      str(i + 1)] = text_Career[i]

                            # print('---------dic-----------------')
                            # print(dic)
                            # print('--------------------------')
                            # print(page, pagesize, total, max_page, degreeName, age, expectedWorkPlaceName)
                            # print('------------result--------------')
                            sql_params = (
                            resumeId, resumeGuid, lastLoginTime, age, expectSalary, career1, career2, career3,
                            dic['workPlace'])
                            self.sql_params_list.append(sql_params)
                            # print(sql_params)

                        if int(total) >= 5000:  # 结果没有全部显示，表明需要增加更多筛选，以便搜索结果数能控制在5000以内
                            if dic['workPlace'] == '':  # 如果当前【期望工作地】没有作为筛选条件
                                for workPlace in self.dicWorkPlace:
                                    print(f'当前结果数大于5000，增加条件workPlace={workPlace}')
                                    dic['workPlace'] = workPlace
                                    params = urlencode(dic)
                                    print(params)
                                    url = f"https://vip.gxrc.com/api/resume/search?{params}"
                                    await self.aiodownload(loop, semaphore, url, cookies, session, is_for_product_url)
                            elif dic['edu'] == '':  # 如果当前【学历】没有作为筛选条件
                                for edu in self.dicEdu:
                                    print(f'当前结果数大于5000，增加条件edu={edu}')
                                    dic['edu'] = edu
                                    params = urlencode(dic)
                                    print(params)
                                    url = f"https://vip.gxrc.com/api/resume/search?{params}"
                                    await self.aiodownload(loop, semaphore, url, cookies, session, is_for_product_url)
                            elif int(dic['ageMin']) == 0:  # 如果当前【年龄】没有作为筛选条件
                                for ageMin in self.dicAge:
                                    ageMax = 0
                                    if int(ageMin) is 0:
                                        ageMax = 20
                                    elif int(ageMin) >= 20:
                                        if int(ageMin) >= 40:
                                            ageMax = 99
                                        else:
                                            ageMax = int(ageMin) + 1
                                    print(f'当前结果数大于5000，增加条件ageMin={ageMin},ageMax={ageMax}')
                                    dic['ageMin'] = ageMin
                                    dic['ageMax'] = ageMax
                                    params = urlencode(dic)
                                    print(params)
                                    url = f"https://vip.gxrc.com/api/resume/search?{params}"
                                    await self.aiodownload(loop, semaphore, url, cookies, session, is_for_product_url)
                            elif int(dic['sex']) == -1:  # 如果当前【性别】没有作为筛选条件
                                for sex in self.dicSex:
                                    print(f'当前结果数大于5000，增加条件sex={sex}')
                                    dic['sex'] = sex
                                    print(dic)
                                    params = urlencode(dic)
                                    print(params)
                                    url = f"https://vip.gxrc.com/api/resume/search?{params}"
                                    await self.aiodownload(loop, semaphore, url, cookies, session, is_for_product_url)
                            elif int(dic['talentDegree']) == 0:  # 如果当前【人才类型】没有作为筛选条件
                                for talentDegree in self.dicTalentDegree:
                                    print(f'当前结果数大于5000，增加条件talentDegree={talentDegree}', threading.currentThread())
                                    dic['talentDegree'] = talentDegree
                                    params = urlencode(dic)
                                    print(params)
                                    url = f"https://vip.gxrc.com/api/resume/search?{params}"
                                    await self.aiodownload(loop, semaphore, url, cookies, session, is_for_product_url)
                            else:
                                print('当前结果数还是大于5000.', threading.currentThread(), dic, resp.request_info.url)
                        else:  # 结果数在5000以内，当前条件符合要求。
                            if is_for_product_url:
                                print('#')
                                # print('结果数在5000以内，当前条件符合要求:', page, pagesize, total, max_page, degreeName, age, expectedWorkPlaceName)
                                for page in range(1,
                                                  int(max_page)):  # 生成1-max_page页，并生成url暂时存放于self.condition_certain_urls
                                    dic['page'] = page
                                    params = urlencode(dic)
                                    # print(params)
                                    url = f"https://vip.gxrc.com/api/resume/search?{params}"
                                    self.condition_certain_urls.append(url)

                            # 有未执行的SQL语句，执行一下
                            if len(self.sql_params_list) > 960:
                                sql_return_msg = await self.insert_to_sqlite(loop, semaphore, self.sql_params_list)
                                self.sql_params_list = []
                                print(sql_return_msg)
                                print('==========================')
            except Exception as e:
                print(f'代理不可用{self.proxy}:{e}')
                await self.get_useable_proxy(session)  # 重新获取新的可用的代理
                await self.aiodownload(loop, semaphore, url, cookies, session, is_for_product_url)  # 重新请求当前程序


        print('sleep 2-5秒')
        asyncio.sleep((random.randrange(2, 5, 1)))
        self.finish_url_count += 1


        # 发送请求
        # 得到图片内容
        # 保存到文件


    async def main(self, loop):
        semaphore = asyncio.Semaphore(10)  # 并发数

        # 1.调用浏览器来登录，人工验证码
        cookies = await self.getCookies()
        customcookies = {}
        for cookie in cookies:
            customcookies.update({cookie['name']: cookie['value']})
        self.clientsession = aiohttp.ClientSession(cookies=customcookies)

        # 2.生成tasks
        tasks = []
        for page in range(1, 2):
            postdata = self.postdata_temp
            postdata['page'] = page

            params = urlencode(postdata)
            # print(params)
            url = f"https://vip.gxrc.com/api/resume/search?{params}"
            # print(url)
            tasks.append(self.aiodownload(loop, semaphore, url, customcookies, self.clientsession, True))
        await asyncio.wait(tasks)
        await self.clientsession.close()

        # 3.把生成的URL一一去请求
        print(fr'共有{len(self.condition_certain_urls)}个url需要请求，现在开始执行……')
        tasks2 = []
        self.clientsession = aiohttp.ClientSession(cookies=customcookies)  # 重新赋值clientsession
        for url in self.condition_certain_urls:
            tasks2.append(self.aiodownload(loop, semaphore, url, customcookies, self.clientsession, False))
        if len(tasks2) > 0:
            await asyncio.wait(tasks2)
            await self.clientsession.close()

        # 4.未执行完的SQL把它执行完
        if len(self.sql_params_list) > 0:
            sql_return_msg = await self.insert_to_sqlite(loop, semaphore, self.sql_params_list)
            self.sql_params_list = []
            print(sql_return_msg)
            print('==========================')



if __name__ == '__main__':
    gxrc = gxrc()

    t1 = time.time()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(gxrc.main(loop))
    t2 = time.time()
    print('over!', t2-t1)
