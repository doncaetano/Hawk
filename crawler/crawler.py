from selenium import webdriver
from bs4 import BeautifulSoup 
from tqdm import tqdm
import pandas as pd
import numpy as np
import time
from secure_keys import DIR
from secure_keys import MAGAZINELUIZA as PACKAGE

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
        self.comment_list = []

    def page_open(self, package_name):
        chrome_options = webdriver.ChromeOptions()
        prefs = {'profile.managed_default_content_settings.images': 2}
        chrome_options.add_experimental_option("prefs", prefs)
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--log-level=3')

        self.driver = webdriver.Chrome(DIR, options=chrome_options)
        self.package_name = package_name
        self.driver.get('https://play.google.com/store/apps/details?id={}&showAllReviews=true'.format(package_name))

    def page_close(self):
        self.driver.quit()

    def page_load(self):
        SCROLL_PAUSE_TIME = 1.5

        while True:
            self.elements = self.driver.find_elements_by_class_name('d15Mdf')
            if(self.elements == []):
                break
            self.load_data()
            elements_drop = self.driver.find_elements_by_css_selector('div[jscontroller="H6eOGe"]')
            for e in elements_drop:
                self.driver.execute_script("arguments[0].remove()",e)
            
            time.sleep(SCROLL_PAUSE_TIME)
            self.driver.execute_script('window.scrollTo(0, document.body.scrollHeight);')
            self.driver.execute_script('window.scrollTo(0, 0);')

            if self.driver.find_elements_by_class_name('CwaK9') != []:
                self.driver.find_element_by_class_name('CwaK9').click()
            time.sleep(SCROLL_PAUSE_TIME)
        
    def load_data(self):
        for element in tqdm(self.elements):
            soup = BeautifulSoup(element.get_attribute('outerHTML'), 'html.parser')

            name = soup.select('span.X43Kjb')[0].get_text()
            date = soup.select('span.p2TkOb')[0].get_text()
            rating = soup.select('div.pf5lIe > div')[0]['aria-label']

            big_review = soup.select('span[jsname="fbQN7e"]')[0].get_text()
            if(big_review == ''):
                review = soup.select('span[jsname="bN97Pc"]')[0].get_text()
            else:
                review = big_review
            self.comment_list.append([name,date,rating,review])
        print(f'list size {len(self.comment_list)}')

    def run_crawler(self, URL):
        self.page_open(URL)
        self.page_load()
        self.page_close()

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
crw.run_crawler(PACKAGE)
df = crw.to_pandas()
df.to_pickle(f'./pickle_files/{crw.package_name}.pkl')



