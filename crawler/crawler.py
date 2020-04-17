from selenium import webdriver
from tqdm import tqdm
import pandas as pd
import numpy as np
import time

EVINO = 'br.com.evino.android'
VIVINO = 'vivino.web.app'
WINE = 'br.com.wine.app'

def reduce_mem_usage(df, verbose=True):
    numerics = ['int16', 'int32', 'int64', 'float16', 'float32', 'float64']
    start_mem = df.memory_usage().sum() / 1024**2    
    for col in df.columns:
        col_type = df[col].dtypes
        if col_type in numerics:
            c_min = df[col].min()
            c_max = df[col].max()
            if str(col_type)[:3] == 'int':
                if c_min > np.iinfo(np.int8).min and c_max < np.iinfo(np.int8).max:
                    df.loc[:,col] = df[col].astype(np.int8)
                elif c_min > np.iinfo(np.int16).min and c_max < np.iinfo(np.int16).max:
                    df.loc[:,col] = df[col].astype(np.int16)
                elif c_min > np.iinfo(np.int32).min and c_max < np.iinfo(np.int32).max:
                    df.loc[:,col] = df[col].astype(np.int32)
                elif c_min > np.iinfo(np.int64).min and c_max < np.iinfo(np.int64).max:
                    df.loc[:,col] = df[col].astype(np.int64)  
            else:
                if c_min > np.finfo(np.float16).min and c_max < np.finfo(np.float16).max:
                    df.loc[:,col] = df[col].astype(np.float16)
                elif c_min > np.finfo(np.float32).min and c_max < np.finfo(np.float32).max:
                    df.loc[:,col] = df[col].astype(np.float32)
                else:
                    df.loc[:,col] = df[col].astype(np.float64)    
    end_mem = df.memory_usage().sum() / 1024**2
    if verbose: print('Mem. usage decreased to {:5.2f} Mb ({:.1f}% reduction)'.format(end_mem, 100 * (start_mem - end_mem) / start_mem))
    return df

class Crawler:
    def __init__(self):
        self.package_name = None
        self.driver = None
        self.elements = None
        self.comment_list = None

    def open_page(self, package_name):
        # load url
        self.driver = webdriver.Chrome('C:\\Users\\Don\\Desktop\\crawler\\chromedriver.exe')
        self.package_name = package_name
        self.driver.get('https://play.google.com/store/apps/details?id={}&showAllReviews=true'.format(package_name))
        self.lang = self.driver.find_element_by_tag_name('html').get_attribute('lang')
        print(self.lang)

    def close_page(self):
        self.driver.close()

    def load_page(self):
        SCROLL_PAUSE_TIME = 2

        last_height = self.driver.execute_script('return document.body.scrollHeight')
        while True:
            # if find a button to load more data then click
            if self.driver.find_elements_by_class_name('CwaK9'):
                time.sleep(SCROLL_PAUSE_TIME)
                self.driver.find_element_by_class_name('CwaK9').click()
                time.sleep(SCROLL_PAUSE_TIME)
            else:
                self.driver.execute_script('window.scrollTo(0, document.body.scrollHeight);')
                time.sleep(SCROLL_PAUSE_TIME)
                new_height = self.driver.execute_script('return document.body.scrollHeight')
                
                if new_height == last_height:
                    break
                last_height = new_height
    
        time.sleep(SCROLL_PAUSE_TIME*2)
        self.elements = self.driver.find_elements_by_class_name('d15Mdf')
    
    def load_data(self):
        self.comment_list = []
        for element in tqdm(self.elements):
            name = element.find_element_by_class_name('X43Kjb').text 
            date = element.find_element_by_class_name('p2TkOb').text
            rating = element.find_element_by_css_selector('div[class="pf5lIe"]>div').get_attribute('aria-label')

            big_review = element.find_element_by_css_selector('span[jsname="fbQN7e"]').get_attribute('textContent')
            if(big_review == ''):
                review = element.find_element_by_css_selector('span[jsname="bN97Pc"]').get_attribute('textContent')
            else:
                review = big_review
            self.comment_list.append([name,date,rating,review])

    def run_crawler(self, URL):
        self.open_page(URL)
        self.load_page()
        self.load_data()
        self.close_page()

    def get_comments(self):
        return self.comment_list

    def to_pandas(self):
        df = pd.DataFrame(self.comment_list, columns=['name','string_date','string_rating','review'])

        df['day'] = df['string_date'].str.slice(start=0, stop=2).str.strip()
        df['month_string'] = df['string_date'].str.slice(start=5).str[:-8].str.strip()
        df['month'] = df['month_string'].map({'abril':4, 'agosto':8, 'dezembro':12, 'fevereiro':2, 'janeiro':1, 'julho':7,
            'junho':6, 'maio':5, 'março':3, 'novembro':11, 'outubro':10, 'setembro':9})
        df['year'] = df['string_date'].str[-4:].str.strip()
        df['date'] = pd.to_datetime((df.year.astype('int32')*10000+df.month.astype('int32')*100+df.day.astype('int32')).apply(str),format='%Y%m%d')

        df.loc[df['string_rating'] == 'Avaliado com 5 de 5 estrelas', 'rating'] = 5
        df.loc[df['string_rating'] == 'Avaliado com 4 de 5 estrelas', 'rating'] = 4
        df.loc[df['string_rating'] == 'Avaliado com 3 de 5 estrelas', 'rating'] = 3
        df.loc[df['string_rating'] == 'Avaliado com 2 de 5 estrelas', 'rating'] = 2
        df.loc[df['string_rating'] == 'Avaliado com 1 de 5 estrelas', 'rating'] = 1

        return reduce_mem_usage(df[['name', 'date', 'rating', 'review']].copy())

crw = Crawler()
crw.run_crawler(EVINO)
df = crw.to_pandas()
df.to_pickle('./{}.pkl'.format(crw.package_name))