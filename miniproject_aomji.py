from airflow import DAG
from airflow.decorators import task ,task_group
from airflow.utils.dates import days_ago

from google.oauth2 import service_account
import time
import bs4
import pandas as pd
from selenium import webdriver
import pandas_gbq
import pendulum

credentials_path = '/opt/airflow/config/de-lab-355709-ac5343e9dbe7.json'
credentials = service_account.Credentials.from_service_account_file(credentials_path)
command_executor_url = 'http://192.168.224.4:4444/wd/hub'
#test
chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument('--ignore-ssl-errors=yes')
chrome_options.add_argument('--ignore-certificate-errors')
chrome_options.add_experimental_option("detach", True)

default_args = {
    'owner':'aom',
    'start_date':pendulum.datetime(2023, 8, 3, 13 , 0, 0, tz="Asia/Bangkok")
}

with DAG(
  dag_id ='minipro_dags_v2',
  default_args=default_args,
  schedule_interval='*/60 8-19 * * *',
  ) as dag:
  @task_group()
  def lotus():
    @task()
    def get_source_page_lotus():
      print('start lotus')
      driver = webdriver.Remote(
      command_executor=command_executor_url,
      options=chrome_options
      )
      time.sleep(5)
      driver.get('https://www.lotuss.com/en/category/milk-and-beverages-1/milk-and-beverages-water/drinking-and-functional-water?sort=relevance:DESC')
      #time.sleep(5)
      #driver.find_element("xpath", '//*[@id="navigation-widget-image-0"]').click()
      time.sleep(10)
      driver.execute_script("document.body.style.zoom='5%'")
      time.sleep(10)
      data_lotus = driver.page_source
      driver.close()
      driver.quit()
      
      return data_lotus
    
    data_lotus = get_source_page_lotus()

    @task()
    def data_transform_lotus(data):
      print('start transform')
      soup = bs4.BeautifulSoup(data)
      all_products_lotus = soup.find_all('div',{'class':'sc-jKTccl bnrSaL'})
      all_products_list_lotus = []
      all_products_price_list_lotus =[]
      for product in all_products_lotus :
            all_products_list_lotus.append(product.find('a',{'class':'sc-cHzqoD jCxdqb two-line'}).text)
            all_products_price_list_lotus.append(product.find('span',{'class':'sc-dkQkyq kUVkFQ'}).text)
      lotus_data = pd.DataFrame([all_products_list_lotus,all_products_price_list_lotus])
      lotus_data = lotus_data.transpose()
      lotus_data.columns =['title','price']
      lotus_data['price'] = lotus_data['price'].astype(float)
      lotus_data['timestamp'] = pd.to_datetime("now").replace(microsecond=0)
      lotus_data['brand'] = lotus_data['title'].str.split(' ').str[0]
      lotus_data['source']  = 'Lotus'
      lotus_data = lotus_data[['title','price','timestamp','brand','source']]
      return lotus_data
    
    df_lotus = data_transform_lotus(data_lotus)

    @task()
    def df_lotus_to_bq(df):
      pandas_gbq.to_gbq(df,'aomji_miniproject.products', project_id='de-lab-355709',if_exists='append',credentials=credentials)
      print('load to bq sucsess')
    
    df_lotus_to_bq(df=df_lotus)

  @task_group()
  def makro():
    @task
    def get_products_makro():
      print('start makro')
      driver = webdriver.Remote(
      command_executor=command_executor_url,
      options=chrome_options
      )
      time.sleep(5)
      driver.get('https://www.makro.pro/en/c/beverages/beverages/drinking-water')
      time.sleep(10)
      #ยอมรับ cookie และผ่านเลข postcode
      try:
        driver.find_element("xpath", '//*[@id="onetrust-accept-btn-handler"]').click()
        time.sleep(3)
        driver.find_element("xpath", '//*[@id=":r2:"]').send_keys("40130")
        time.sleep(3)
        driver.find_element("xpath", '/html/body/div[3]/div[3]/div/div/div[2]/div/p[1]').click()
        time.sleep(3)
        driver.find_element("xpath",'/html/body/div[3]/div[3]/div/div/div[2]/div/div/div/button').click()
      except:
        driver.find_element("xpath", '//*[@id=":r2:"]').send_keys("40130")
        time.sleep(3)
        driver.find_element("xpath", '/html/body/div[2]/div[3]/div/div/div[2]/div/p[1]').click()
        time.sleep(3)
        driver.find_element("xpath",'/html/body/div[2]/div[3]/div/div/div[2]/div/div/div/button').click()
    
      all_products_list_makro= []
      all_products_price_makro = []
      all_products_brand_makro = []
      i = 1 
      x = 4 
      #loop page
      while i  <= 100 :
      #delay เพื่อดึงข้อมูล
        time.sleep(2)
        data_makro = driver.page_source
        soup = bs4.BeautifulSoup(data_makro)
        time.sleep(2)
        print('round =',i)
        i += 1
        all_products_makro = soup.find_all('div',{'class':'MuiPaper-root MuiPaper-elevation MuiPaper-rounded MuiPaper-elevation0 css-zmnxdm'})
        for product  in all_products_makro :
          all_products_list_makro.append(product.find('div',{'class':'css-15q0rut'}).text)
          all_products_brand_makro.append(product.find('p',{'class':'MuiTypography-root MuiTypography-body1 css-14hstrx'}).text)
      # เงื่อนไข การดึงราคา
          if product.find('p',{'class':'MuiTypography-root MuiTypography-body1 plp_v2 css-1mo50vk'}) is not None and \
            product.find('p',{'class':'MuiTypography-root MuiTypography-body1 plp_v2 css-1jvp2ji'}) is None : 
            all_products_price_makro.append(product.find('p',{'class':'MuiTypography-root MuiTypography-body1 plp_v2 css-1mo50vk'}).text)
          elif product.find('p',{'class':'MuiTypography-root MuiTypography-body1 plp_v2 css-1jvp2ji'}) is  not None and  \
            product.find('p',{'class':'MuiTypography-root MuiTypography-body1 plp_v2 css-1mo50vk'}) is  None:
            all_products_price_makro.append(product.find('p',{'class':'MuiTypography-root MuiTypography-body1 plp_v2 css-1jvp2ji'}).text)
                      
        # เงื่อนไข การดึงราคา(สินค้าที่หมดแล้ว)
          elif product.find('p',{'class':'MuiTypography-root MuiTypography-body1 plp css-1mo50vk'}) is not None and \
            product.find('p',{'class':'MuiTypography-root MuiTypography-body1 plp css-1jvp2ji'}) is None : 
            all_products_price_makro.append(product.find('p',{'class':'MuiTypography-root MuiTypography-body1 plp css-1mo50vk'}).text)
          elif product.find('p',{'class':'MuiTypography-root MuiTypography-body1 plp css-1jvp2ji'}) is  not None and  \
            product.find('p',{'class':'MuiTypography-root MuiTypography-body1 plp css-1mo50vk'}) is  None:
            all_products_price_makro.append(product.find('p',{'class':'MuiTypography-root MuiTypography-body1 plp css-1jvp2ji'}).text)
        
    #เงื่อนไข การออกจากลูป  
        try :
          driver.find_element("xpath",f'//*[@id="__next"]/div[2]/div[2]/div[2]/div[2]/div[2]/div[2]/nav/ul/li[{x}]/button').click()
          print('next = ',int(x)-3)
          x += 1
          time.sleep(5)
        except  :
          break

      print('get data sucsess')
      driver.close()
      driver.quit()
      makro_data = pd.DataFrame([all_products_list_makro,all_products_price_makro,all_products_brand_makro])
      #เปลี่ยนเป็นแนวตั้ง
      makro_data = makro_data.transpose()
      #จัด columns ตั้งชื่อ ตั้ง type
      makro_data.columns =['title','price','brand']
      makro_data["price"] = makro_data.apply(lambda x: x["price"].replace("฿",""), axis=1)
      makro_data["price"] = makro_data["price"].astype(float)
      makro_data['timestamp'] =  pd.to_datetime("now").replace(microsecond=0)
      makro_data['source']  = 'Makro'
      makro_data =makro_data[['title','price','timestamp','brand','source']]
      print('transfrom sucsess')
      return makro_data

    df_makro = get_products_makro()
    @task()

    def df_makro_to_bq(df):
      pandas_gbq.to_gbq(df,'aomji_miniproject.products', project_id='de-lab-355709',if_exists='append',credentials=credentials)
      print('load to bq sucsess')
    
    df_makro_to_bq(df=df_makro)
         
  lotus()>>makro()