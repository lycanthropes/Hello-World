# -*- coding: utf-8 -*-
"""
Created on Sun Aug 12 13:23:14 2018

@author: Administrator
"""
#  本脚本是为实现自动下载JF期刊2017-2018年的文章而写
# 本代码须用有权限下载JF期刊的IP访问，为避免法律纠纷，本脚本不提供代理IP.（本脚本在校园网IP内可以合法访问和下载）

import time
import csv
import os
from selenium import webdriver
#from pyvirtualdisplay import Display
import requests
import re
from requests.exceptions import RequestException 

# 得到每一期主页的网址
def get_issue_url(begin_year,end_year):
    issue_url_list=[]
    for j in range(begin_year,end_year):
        year_num=str(j)
        VOL_num=str(j-1945)
        for i in range(1,7):
            issue_num=str(i)
            issue_url='http://onlinelibrary.wiley.com/toc/15406261/%s/%s/%s'%(year_num,VOL_num,issue_num)
            issue_url_list.append(issue_url)
    return issue_url_list[:]


def get_pdf_url(issue_url):
    driver=webdriver.Chrome()
    driver.get(issue_url)
    time.sleep(5)
    Issue_num = driver.find_elements_by_xpath('//div[@class="cover-image__parent-item"]')[0].text[10:]
    Vol_num = driver.find_elements_by_xpath('//span[@class="comma"]')[0].text[:-1]
    year_num = driver.find_elements_by_xpath('//div[@class="issue-item"]//li[@class="ePubDate"]/span[last()]')[0].text[-4:]
    one_issue_pdf_url_list = []
    one_issue_file_title_list=[]
    one_issue_title_list=[]
    one_issue_page_list=[]
    for i,link in enumerate(driver.find_elements_by_xpath(' //*[@class="card issue-items-container"]/*[@title="ARTICLES"]/../div')):
        url=link.find_elements_by_xpath('//*[@class="card issue-items-container"]/*[@title="ARTICLES"]/../div/*[@class="issue-item__title visitable"]')[i].get_attribute('href')
        url1,url2=url.split("doi")
        url=url1+"doi/pdf"+url2
        title=link.find_elements_by_xpath('//*[@class="card issue-items-container"]/*[@title="ARTICLES"]/../div//h2')[i].text
        page=link.find_elements_by_xpath('//*[@class="card issue-items-container"]/*[@title="ARTICLES"]/../div//li[@class="page-range"]/span[last()]')[i].text
        file_title=year_num+'-'+Issue_num+'-'+Vol_num+'-'+page
        one_issue_pdf_url_list.append(url)
        one_issue_file_title_list.append(file_title)
        one_issue_title_list.append(title)
        one_issue_page_list.append(page)
    return one_issue_pdf_url_list,one_issue_file_title_list,one_issue_title_list,one_issue_page_list


def save_pdf(url,title):
    try:
        con=requests.get(url)
        if con.status_code==200:
            pass
        else:
            print('爬虫被封，再试一次')
            save_pdf(url,title)
    except  RequestException:
        print('请求索引页错误')
        save_pdf(url,title)
    file_path='{0}/{1}.{2}'.format(os.getcwd(),title,'pdf')
    if not os.path.exists(file_path):
        print(file_path)
        with open(file_path,'wb') as f:
            f.write(con.content)
            f.close()
            
            
          
if __name__=='__main__':
    begin_year=2017
    end_year=2018
    issue_url_list=get_issue_url(begin_year,end_year)
    
    
    for issue_url in issue_url_list:
       one_issue_pdf_url_list,one_issue_file_title_list, one_issue_title_list, one_issue_page_list=get_pdf_url(issue_url)
       titles=[]
       for pdf_title in one_issue_title_list:
           regularname=re.sub('[\/:*?"<>|]','',pdf_title)
           titles.append(regularname)
       for i,pdf_url in enumerate(one_issue_pdf_url_list):
           save_pdf(pdf_url,titles[i])
          

    
    