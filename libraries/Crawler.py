#!/usr/bin/env python
# coding: utf-8

# In[1]:


from selenium import webdriver
import os
import pandas as pd
curr_dir=os.getcwd()

meteoblue_path="\\download\\Meteoblue_data\\"
device_path="\\download\\IOT_data\\"


# # Flush Memory

# In[2]:


def flush_memo():
    fldrs=[meteoblue_path.replace("\\","/")[1:],device_path.replace("\\","/")[1:]]
    
    for fldr in fldrs:
        files=os.listdir(fldr)
        for file in files:
            #print(file)
            os.remove(fldr+'/'+file)


# # Crawl Meteoblue

# In[3]:


def crawler_meteoblue():
    chromeOptions=webdriver.ChromeOptions()
    
    prefs = {"download.default_directory" : curr_dir+meteoblue_path,
            "directory_upgrade": True}
    
    chromeOptions.add_experimental_option("prefs",prefs)
    
    driver = webdriver.Chrome(executable_path='chromedriver/chromedriver.exe',
                              chrome_options=chromeOptions)
    
    driver.get("https://www.meteoblue.com/en/products/historyplus/download/durgapur_india_1272175")

    driver.find_element_by_xpath("/html/body/div[2]/div/form/div/input").click() #accept and continue

    button=driver.find_element_by_class_name("bloo")

    ######Click on checkbox#########
    
    def click_element(driver,e_xpath):
        while True:
            try:
                elem=driver.find_element_by_xpath(e_xpath)
                elem.click()
                return
            except:
                pass

    l=['//*[@id="params"]/optgroup[2]/option[3]',
       '//*[@id="params"]/optgroup[6]/option[1]',
       '//*[@id="params"]/optgroup[4]/option[1]',
       '//*[@id="params"]/optgroup[4]/option[2]',
       '//*[@id="params"]/optgroup[3]/option[2]',
       '//*[@id="params"]/optgroup[3]/option[4]',
       '//*[@id="params"]/optgroup[3]/option[3]']
    
    for e in l:
        click_element(driver,e)
            
    click_element(driver,'//*[@id="wrapper-main"]/div/main/div/div[2]/form/div[4]/div[3]/div/input')
    
    return driver


# In[4]:


#meteo_blue_site=crawler_meteoblue()


# # Process Meteoblue

# In[5]:


def pre_process_meteoblue():
    import pandas as pd
    import numpy as np
    import os
    files=os.listdir(curr_dir+meteoblue_path)
    for file in files:
        fle=file
    df=pd.read_excel(curr_dir+meteoblue_path+fle)
    a=df.iloc[8]
    arr=np.array(df.iloc[9:,:])
    a=np.array(df.iloc[8,:])

    l=[]
    l.append(a[0])
    for i in a:
        if 'timestamp' in i:
            continue
        else:
            l.append(i[9:])
    final=pd.DataFrame(arr,columns=l)
    required=['timestamp','Wind Speed [10 m]',
              'Wind Direction [10 m]',
              'Wind Gust','Cloud Cover High [high cld lay]',
              'Mean Sea Level Pressure [MSL]']
    req=final[required]
    
    cols=['timestamp',
          'Wind Speed  [10 m above gnd]',
          'Wind Direction  [10 m above gnd]',
          'Wind Gust  [sfc]',
          'High Cloud Cover  [high cld lay]',
          'Mean Sea Level Pressure  [MSL]']
    
    req_a=np.array(req)
    final_n=pd.DataFrame(req_a,columns=cols)
    return final_n#.to_csv('temp_meteoblue.csv',index=False)


# In[6]:


#meteo_data=pre_process_meteoblue()


# # Crawl IOT Device

# In[7]:


def crawler_IOT():
    chromeOptions=webdriver.ChromeOptions()
    
    prefs = {"download.default_directory" : curr_dir+device_path,
            "directory_upgrade": True}
    
    chromeOptions.add_experimental_option("prefs",prefs)
    
    driver = webdriver.Chrome(executable_path='chromedriver/chromedriver.exe',
                              chrome_options=chromeOptions)
    
    driver.get("http://iotbuilder.in/nit-dp/dashboard.php")
    button=driver.find_element_by_class_name("col-md-12")
    butt=button.find_element_by_class_name("text-center")
    links = driver.find_elements_by_link_text('Save')
    for link in links:
        link.click()
        
    while sum([os.path.exists(curr_dir+device_path+f'Device-{e}.xls') for e in range(1,8)])<7:
        pass #wait for the devices to complete download
        
    return driver


# In[8]:


#iot_site=crawler_IOT()


# In[9]:


#close drivers
#meteo_blue_site.close()
#iot_site.close()


# # Process IOT Device data

# In[10]:


def preprocess_IOT(meteo_data,date_in):
    fldr=device_path.replace("\\","/")[1:-1]
    files=os.listdir(fldr)
    devices_data={}
    devices=['Device-1.xls','Device-2.xls','Device-3.xls','Device-4.xls']
    for file in devices:
        ac=fldr+'/'+file
        #print(ac)
        df=pd.read_html(ac)
        date=[]
        df_ac=df[0]
        i=0
        while i<len(df_ac):
            date.append(df_ac.iloc[i]['Date'].split(':')[0])
            i+=1
        df_ac['Date_req']=date
        if 'NA' in date_in:
            d_temp=df_ac.iloc[0]['Date_req']
            #print("A")
        else:
            d_temp=date_in
            #print("B")
        df_temp=df_ac[df_ac['Date_req']==d_temp]
        time_stamp=d_temp
        reqs=['Temperature','Humidity']
        df_t=df_temp[reqs]
        means=df_t.mean(axis=0)

        number=(file.split('.')[0]).split('-')[1]
        devices_data[number]={}
        devices_data[number]['Time']=time_stamp
        devices_data[number]['Temperature']=means['Temperature']
        devices_data[number]['Humidity']=means['Humidity']

    data_df=pd.DataFrame(devices_data) 
    df_f=data_df.transpose()
    #print(df_f)
    df_f=df_f.reset_index()      
    df1=meteo_data#pd.read_csv("temp_meteoblue.csv")        
    i=0
    date_req=[]
    while i<len(df1):
        date_req.append(str(df1.iloc[i]['timestamp']).split(':')[0])
        i+=1
    df1['Time']=date_req
    df_com=df1[df1['Time']==d_temp]
    result = df_f.merge(df_com, on='Time',how='left')
    result=result.drop(['Time'],axis=1)

    return result#.to_csv('Final.csv',index=False)


# In[11]:


#preprocess_IOT(meteo_data,'2020-12-26 11')


# # Overall Crawler

# In[12]:


def crawler(date_in=None):
    #1.
    flush_memo()
    
    #2.
    meteo_site=crawler_meteoblue()
    
    #3.
    iot_dev_site=crawler_IOT()
    
    #23~
    meteo_site.close()
    iot_dev_site.close()
    
    #4.
    meteo_data=pre_process_meteoblue()
    
    #5.
    iot_dev_data=preprocess_IOT(meteo_data,date_in)
    
    #6.
    return iot_dev_data


# In[13]:


#crawler('2020-12-24 11')


# In[14]:


#NICE

