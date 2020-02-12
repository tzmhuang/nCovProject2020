import sys
import os
import time
import datetime
import random
import json
from queue import Queue

from scrapy import Request, Selector
from selenium import webdriver
PROJECT_ROOT = os.path.abspath(os.path.dirname(__file__))
WEBDRIVER = os.path.join(PROJECT_ROOT, "webdriver/chromedriver")
SAVE_DIR = os.path.join(PROJECT_ROOT, "rawdata/baiduqianxi/")


class baiduqianxiSpider():
    def __init__(self):
        self.name = 'baiduqianxi'
        self.crawl_id = datetime.datetime.now().strftime('%S%M%H%d%m%Y')
        self.SAVE_DIR = os.path.join(SAVE_DIR, self.crawl_id)
        self.CHECKPOINT_DIR = os.path.join(self.SAVE_DIR, "checkpoint/")
        if not os.path.exists(self.CHECKPOINT_DIR):
            os.makedirs(self.CHECKPOINT_DIR)

        self._checkpts = Queue()
        self._browser = webdriver.Chrome(executable_path=WEBDRIVER)

    def crawl(self):
        self.start_requests()
        self._browser.close()

    def start_requests(self):
        self.start_url = 'https://qianxi.baidu.com'
        self._browser.get(self.start_url)
        print("LOADING...")
        time.sleep(2)
        print("DONE LOADING")
        self.parse_main()

    def parse_main(self):
        # select date
        date_list = self._browser.find_elements_by_xpath(
            '//ul[@class="hui-option-list"]/li')
        data_type_list = self._browser.find_elements_by_xpath(
            '//div[contains(@class,"button_group primary")]/div')
        city_list = self._browser.find_elements_by_xpath(
            '//a[@class="sel_city_name"]')
        national_element = self._browser.find_element_by_xpath(
            '//div[@id="hot_city_ids"]/a[@name="全国"]')
        city_list[0] = national_element
        date_dict = {}
        for idx, date_element in enumerate(date_list):
            assert self._click_element(date_element)
            date = self._browser.find_element_by_xpath(
                '//span[@class="hui-option-tips"]/span').text
            print('PARSING: ', date)
            # self._sleep_random(0.05)
            datatype_dict = {}
            for datatype_element in data_type_list:
                assert self._click_element(datatype_element)
                data_type = datatype_element.text
                print('PARSING: ', data_type)
                self._sleep_random(0.05)
                citydata_dict = {}
                for city_element in city_list:
                    assert self._click_element(city_element)
                    city = self._browser.find_element_by_id(
                        'cur_city_name').text
                    print('PARSING: ', city)
                    self._sleep_random(0.08)
                    table = self._get_table_from_selector(
                        Selector(text=self._browser.page_source))
                    citydata_dict[city] = list(table)
                datatype_dict[data_type] = citydata_dict
            date_dict[date] = datatype_dict
            self._checkpoint(date_dict, self.CHECKPOINT_DIR)
        self._dump_json(date_dict, self.SAVE_DIR)

    def _click_element(self, element):
        try:
            self._browser.execute_script('arguments[0].click()', element)
            return True
        except:
            print('Click Failed: ', element)
            return False

    def _get_table_from_selector(self, selector):
        CITY_DATA_LIST_SELECTOR = '//div[@class="mgs-list-box"]//table/tbody/tr'
        DATE_SELECTOR = '//span[@class="hui-option-tips"]/span/text()'
        DATA_DESCRIPTION = '//div[@class="radio active"]/text()'
        date = selector.xpath(DATE_SELECTOR)
        for city_data in selector.xpath(CITY_DATA_LIST_SELECTOR):
            CITY_NAME_SELECTOR = './/div/span[@class="mgs-date-city"]/text()'
            PROVINCE_NAME_SELECTOR = './/div/span[@class="mgs-date-province"]/text()'
            DATA_SELECTOR = './td[not(./span) and not(./div)]/text()'
            datalet = {
                'province': city_data.xpath(PROVINCE_NAME_SELECTOR).extract_first(),
                'city': city_data.xpath(CITY_NAME_SELECTOR).extract_first(),
                'data': city_data.xpath(DATA_SELECTOR).extract_first()
            }
            yield datalet

    def _sleep_random(self, upper_limit):
        random.randrange(0, upper_limit*100, 1)/100
        return

    def _checkpoint(self, dict, DIR):
        checkpt_name = self._dump_json(dict, DIR)
        self._checkpts.put(checkpt_name)
        max_checkpt_num = 3
        if self._checkpts.qsize() >= max_checkpt_num:
            os.remove(self._checkpts.get())
        print('CHECKPOINT REACHED: ', checkpt_name)

    def _dump_json(self, dict, DIR):
        file_name = os.path.join(
            DIR, datetime.datetime.now().strftime("%S%M%H_%d_%m_%Y")+'.json')
        if os.path.exists(file_name):
            raise ("File already exists")
        with open(file_name, 'w', encoding='utf-8') as f:
            json.dump(dict, f, ensure_ascii=False, indent=4)
        return file_name


if __name__ == "__main__":
    spider = baiduqianxiSpider()
    spider.crawl()
