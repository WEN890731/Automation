#!/usr/bin/env python
# coding: utf-8

# In[2]:


import pandas as pd
import datetime
from dateutil.relativedelta import relativedelta
import requests
import json
from main import GoogleAPIClient
import tkinter as tk
from tkinter import messagebox
from tkinter import filedialog
from tkinter import *
from tkinter import ttk
import pandas as pd
from PIL import ImageTk,Image
import webbrowser


# In[3]:


class GoogleSheets(GoogleAPIClient):
    def __init__(self) -> None:
        # 呼叫 GoogleAPIClient.__init__()，並提供 serviceName, version, scope
        super().__init__(
            'sheets',
            'v4',
            ['https://www.googleapis.com/auth/spreadsheets'],
        )

    def getWorksheet2Df(self, spreadsheetId: str, range: str):#獲取指定google sheet的檔案，輸出成dataframe
        request = self.googleAPIService.spreadsheets().values().get(
            spreadsheetId=spreadsheetId,
            range=range,
        )
        result = request.execute()['values']
        header = result[0]
        del result[0]
        return pd.DataFrame(result, columns=header)
    
    def getWorksheet(self, spreadsheetId: str, range: str):#獲取指定google sheet的檔案，輸出成list
        request = self.googleAPIService.spreadsheets().values().get(
            spreadsheetId=spreadsheetId,
            range=range,
        )
        response = request.execute()['values']
        return response

    def clearWorksheet(self, spreadsheetId: str, range: str):#清除指定google sheet的資料
        self.googleAPIService.spreadsheets().values().clear(
            spreadsheetId=spreadsheetId,
            range=range,
        ).execute()
        return 'done'
    
    def setWorksheet(self, spreadsheetId: str, range: str, df: pd.DataFrame):#清除指定google sheet的資料後新增資料(dataframe)
        self.clearWorksheet(spreadsheetId, range)
        #使用 update 的語法把我們新的數據加入至 Google Sheets
        self.googleAPIService.spreadsheets().values().update(
            spreadsheetId=spreadsheetId,
            range=range,
            valueInputOption='USER_ENTERED',
            body={
                'majorDimension': 'ROWS',
                'values': df.T.reset_index().T.values.tolist()
            },
        ).execute()
        return 'done'

    def appendWorksheet(self, spreadsheetId: str, range: str, df: pd.DataFrame):#對指定google sheet加入資料(dataframe)
        self.googleAPIService.spreadsheets().values().append(
            spreadsheetId=spreadsheetId,
            range=range,
            valueInputOption='USER_ENTERED',
            body={
                'majorDimension': 'ROWS',
                'values': df.values.tolist()
            },
        ).execute()
        return 'done'
    


# In[4]:


def trafficGet(curBrand):
    dat = datetime.datetime.now()
    lastMonth = dat - relativedelta(months=2)
    lastMonth4api = lastMonth.strftime('%Y-%m')
    brand = curBrand

    url = "https://api.similarweb.com/v1/website/%s.com.tw/traffic-sources/overview-share?api_key=7e552f683942499fbd752ad182af1dab&start_date=%s&end_date=%s&country=tw&granularity=monthly&main_domain_only=false&format=json&mtd=false"%(brand, lastMonth4api, lastMonth4api)
    headers = {"accept": "application/json"}
    response = json.loads(requests.get(url, headers=headers).text)

    curUrl = brand + '.com.tw'
    trafficData = response['visits'][curUrl][0]['visits'][0]['organic']
    return trafficData

def classti(brand, oriDf):
    oriDf['URL'] = oriDf['URL'].str.lower()
    
    if brand=='518':
        #辨識其他頁面
        other518 = GoogleSheets().getWorksheet2Df(spreadsheetId ='1A5KNVZfICR9UveguQ9uqAVfZZ0qKDEY986B2lkUBljM',
                                range = '518other')
        for i in range(len(other518.reg)):
            oriDf.loc[(oriDf['URL'].str.contains(other518.reg[i])), 'category'] = '其他頁面'
        #網頁群組分類
        regex518 = GoogleSheets().getWorksheet2Df(spreadsheetId ='1A5KNVZfICR9UveguQ9uqAVfZZ0qKDEY986B2lkUBljM',
                                range = '518')
        catNum = len(regex518.cat)
        for i in range(catNum):
            if regex518.cat[i]=='03.職缺頁':
                searchResReg = regex518.loc[(regex518['cat'].str.contains('04.職缺搜尋結果'))].reg.values[0]
                oriDf.loc[~(oriDf['URL'].str.contains(searchResReg)) & (oriDf['URL'].str.contains('^518.com.tw/job-')), 'category'] = '03.職缺頁'
                oriDf.loc[~(oriDf['URL'].str.contains(searchResReg)) & (oriDf['URL'].str.contains('^518.com.tw/job-')), 'index'] = 3
            else:
                oriDf.loc[(oriDf['URL'].str.contains(regex518.reg[i])), 'category'] = regex518.cat[i]
                oriDf.loc[(oriDf['URL'].str.contains(regex518.reg[i])), 'index'] = int(regex518.indexC[i])
        #辨識未分類頁面
        oriDf.loc[oriDf.category.isnull(), 'category'] = '未分類'
        
        oriDf.loc[oriDf['category']=='未分類', 'index'] = 0
        oriDf.loc[oriDf['category']=='其他頁面', 'index'] = len(regex518)+1
    
    elif brand=='1111':
        #辨識其他頁面
        other1111 = GoogleSheets().getWorksheet2Df(spreadsheetId ='1A5KNVZfICR9UveguQ9uqAVfZZ0qKDEY986B2lkUBljM',
                                range = '1111other')
        for i in range(len(other1111.reg)):
            oriDf.loc[(oriDf['URL'].str.contains(other1111.reg[i])), 'category'] = '其他頁面'
        #網頁群組分類
        regex1111 = GoogleSheets().getWorksheet2Df(spreadsheetId ='1A5KNVZfICR9UveguQ9uqAVfZZ0qKDEY986B2lkUBljM',
                                range = '1111')
        catNum = len(regex1111.cat)
        for i in range(catNum):
            oriDf.loc[(oriDf['URL'].str.contains(regex1111.reg[i])), 'category'] = regex1111.cat[i]
            oriDf.loc[(oriDf['URL'].str.contains(regex1111.reg[i])), 'index'] = int(regex1111.indexC[i])
        #辨識未分類頁面   
        oriDf.loc[oriDf.category.isnull(), 'category'] = '未分類'
        
        oriDf.loc[oriDf['category']=='未分類', 'index'] = 0
        oriDf.loc[oriDf['category']=='其他頁面', 'index'] = len(regex1111)+1
        
    elif brand=='104':
        #辨識其他頁面
        other104 = GoogleSheets().getWorksheet2Df(spreadsheetId ='1A5KNVZfICR9UveguQ9uqAVfZZ0qKDEY986B2lkUBljM',
                                range = '104other')
        for i in range(len(other104.reg)):
            oriDf.loc[(oriDf['URL'].str.contains(other104.reg[i])), 'category'] = '其他頁面'
        #網頁群組分類
        regex104 = GoogleSheets().getWorksheet2Df(spreadsheetId ='1A5KNVZfICR9UveguQ9uqAVfZZ0qKDEY986B2lkUBljM',
                                range = '104')
        catNum = len(regex104.cat)
        for i in range(catNum):
            if regex104.cat[i]=='招募管理':
                searchResReg = regex104.loc[(regex104['cat'].str.contains('人資充電'))].reg.values[0]
                oriDf.loc[~(oriDf['URL'].str.contains(searchResReg)) & (oriDf['URL'].str.contains('^pro.104.com.tw')), 'category'] = '招募管理'
                oriDf.loc[~(oriDf['URL'].str.contains(searchResReg)) & (oriDf['URL'].str.contains('^pro.104.com.tw')), 'index'] = 25
            else:
                oriDf.loc[(oriDf['URL'].str.contains(regex104.reg[i])), 'category'] = regex104.cat[i]
                oriDf.loc[(oriDf['URL'].str.contains(regex104.reg[i])), 'index'] = int(regex104.indexC[i])
        #辨識未分類頁面
        oriDf.loc[oriDf.category.isnull(), 'category'] = '未分類'
        
        oriDf.loc[oriDf['category']=='未分類', 'index'] = 0
        oriDf.loc[oriDf['category']=='其他頁面', 'index'] = len(regex104)+1
        
    else:
        oriDf['category'] = ['目前暫無此網頁分類規則']*len(oriDf)
        oriDf['index'] = [0]*len(oriDf)

    oriDf = oriDf.sort_values(by=['index'])
    #日期欄位設定
    dat = datetime.datetime.now()
    lastMonth = dat - relativedelta(months=1)
    lastMonthStr = lastMonth.strftime('%b-%y')
    oriDf['date'] = lastMonthStr
    
    #計算各網頁traffic
    tra = trafficGet(brand)
    oriDf['traffic'] = oriDf['Traffic Share ']*tra
    
    #欄位順序調整
    oriDf = oriDf[['date', 'URL', 'Traffic Share ', 'Keywords', 'Top Keyword', 'category', 'traffic']]
    
    return oriDf


# In[5]:


class Application(tk.Frame):
    def __init__(self, master=None):
        super().__init__(master)
        self.master = master
        self.master.title("人力銀行網頁URL分類程式")
        self.master.configure(background='midnightblue')
        self.master.geometry("620x250")
        self.master.resizable(0,0)
        self.create_widgets()
        global FILEBRAND, FILEDATE

    def create_widgets(self):
        self.curDate_Label = tk.Label(self.master, text='最新資料日期: ', bg='sandybrown', font=('Arial', 12, 'bold'), width=30, height=2)
        self.curDate_Label.grid(row=0, column=1, padx=10, pady=10,sticky=W,rowspan=1)
        
        self.excel_label = tk.Label(self.master, text="原始Excel檔：")
        self.excel_label.grid(row=1, column=1, padx=10, pady=10,sticky=W+N,rowspan=2)
        
        self.excel_button = tk.Button(self.master, text="選擇檔案", command=self.choose_file, width=20, height=1)
        self.excel_button.grid(row=1, column=1, padx=10, pady=10, rowspan=2,sticky=E+N)
 
        self.brand_label = tk.Label(self.master, text="當前品牌：")
        self.brand_label.grid(row=2, column=1, padx=10, pady=10, rowspan=1,sticky=W)
        
        self.brand_label = tk.Label(self.master, text="當前資料日期：")
        self.brand_label.grid(row=2, column=1, padx=10, pady=10, rowspan=4,sticky=W)
        
        self.submit_button = tk.Button(self.master, text="匯入Excel檔", command=self.load_excel, width=30, height=1)
        self.submit_button.grid(row=4, column=1, padx=10, pady=10,sticky=W+E, rowspan=3)
                
        img_path = "C:/Users/wendywen/python code/06-202208-AutoUpdateKPIReport/sw檔案/logo/r.png"
        img = ImageTk.PhotoImage(file=img_path)
        self.img_label = tk.Label(root, image=img)
        self.img_label.image = img
        self.img_label.grid(row=0, column=0, sticky=W+E+N+S, rowspan=6)
        
    def choose_file(self):
        filetypes = (("Excel files", "*.xlsx"), ("All files", "*.*"))
        self.filename = filedialog.askopenfilename(filetypes=filetypes)
        self.FILEBRAND, self.FILEDATE = self.catchBrandAndDate(self.filename)
        curDateStr = self.curDataDat(self.FILEBRAND)
        
        if self.FILEBRAND != '無法辨識品牌' and self.FILEDATE != '無法辨識日期':
            self.curDate_Label = tk.Label(self.master, text='最新資料日期: '+curDateStr, bg='sandybrown', font=('Arial', 12, 'bold'), width=30, height=2)
            self.curDate_Label.grid(row=0, column=1, padx=10, pady=10,sticky=W,rowspan=1)
            
            self.excel_button.configure(text='匯入成功')
            
            self.fileBrand_label = tk.Label(self.master, text=self.FILEBRAND,width=10, height=1, font=('Arial', 10, 'bold'), fg = 'aliceblue', background = 'midnightblue')
            self.fileBrand_label.grid(row=2, column=1, padx=10, pady=10, rowspan=1,sticky=E)
            
            self.fileDat_label = tk.Label(self.master, text=self.FILEDATE,width=10, height=1, font=('Arial', 10, 'bold'), fg = 'aliceblue', background = 'midnightblue')
            self.fileDat_label.grid(row=2, column=1, padx=10, pady=10, rowspan=4,sticky=E)
            
        elif self.FILEDATE == '無法辨識日期':
            
            self.curDate_Label = tk.Label(self.master, text='最新資料日期: '+curDateStr, bg='sandybrown', font=('Arial', 12, 'bold'), width=30, height=2)
            self.curDate_Label.grid(row=0, column=1, padx=10, pady=10,sticky=W,rowspan=1)
            
            self.excel_button.configure(text='匯入失敗')
            
            self.fileBrand_label = tk.Label(self.master, text=self.FILEBRAND, fg = 'aliceblue', background = 'midnightblue')
            self.fileBrand_label.grid(row=2, column=1, padx=10, pady=10, rowspan=1,sticky=E)

            self.fileDat_label = tk.Label(self.master, text=self.FILEDATE, fg = 'aliceblue', background = 'midnightblue')
            self.fileDat_label.grid(row=2, column=1, padx=10, pady=10, rowspan=4,sticky=E)
            
        elif self.FILEBRAND == '無法辨識品牌':
            
            self.curDate_Label = tk.Label(self.master, text='最新資料日期: '+curDateStr, bg='sandybrown', font=('Arial', 12, 'bold'), width=30, height=2)
            self.curDate_Label.grid(row=0, column=1, padx=10, pady=10,sticky=W,rowspan=1)
            
            self.excel_button.configure(text='匯入失敗')
            
            self.fileBrand_label = tk.Label(self.master, text=self.FILEBRAND, fg = 'aliceblue', background = 'midnightblue')
            self.fileBrand_label.grid(row=2, column=1, padx=10, pady=10, rowspan=1,sticky=E)

            self.fileDat_label = tk.Label(self.master, text=self.FILEDATE, fg = 'aliceblue', background = 'midnightblue')
            self.fileDat_label.grid(row=2, column=1, padx=10, pady=10, rowspan=4,sticky=E)
            
        else :
            self.excel_button.configure(text='匯入失敗')
            
            self.fileBrand_label = tk.Label(self.master, text=self.FILEBRAND, fg = 'aliceblue', background = 'midnightblue')
            self.fileBrand_label.grid(row=2, column=1, padx=10, pady=10, rowspan=1,sticky=E)

            self.fileDat_label = tk.Label(self.master, text=self.FILEDATE, fg = 'ghostblue', background = 'midnightblue')
            self.fileDat_label.grid(row=2, column=1, padx=10, pady=10, rowspan=4,sticky=E)
            
    def catchBrandAndDate(self, fileName):
        fileName = fileName.replace('.xlsx', '')
        try:
            dat = datetime.datetime.strptime(fileName[-8:-1], '%Y.%m')
            datReshape = datetime.datetime.strftime(dat, '%b-%y')
        except ValueError:
            datReshape = '無法辨識日期'
            
        if fileName.find('-(') != -1 and fileName.find('.com') != -1:
            brand = fileName[fileName.find('-(')+2:fileName.find('.com')]
        else:
            brand = '無法辨識品牌'
        return brand, datReshape

    def load_excel(self):
        try:
            oriDf = pd.read_excel(self.filename, sheet_name = 'Landing_Pages')
            reshapeDf = classti(self.FILEBRAND, oriDf)
            
            
            try:
                orgDf = GoogleSheets().getWorksheet2Df(
                                            spreadsheetId='1YWi8hg545ryn_KlWUSp2jDKt85BZ1ACHGw67iwIyrgY',
                                            range = self.FILEBRAND)

                #正式版
                GoogleSheets().setWorksheet(
                                    spreadsheetId='1YWi8hg545ryn_KlWUSp2jDKt85BZ1ACHGw67iwIyrgY',
                                    range=self.FILEBRAND ,
                                    df = pd.concat([reshapeDf, orgDf], axis=0)
                                )
                #副總版
                GoogleSheets().setWorksheet(
                                    spreadsheetId='1eNM9ptlGi4BUKInshOg2dOauso6INK8uWJO-Cz7IGIU',
                                    range=self.FILEBRAND ,
                                    df = pd.concat([reshapeDf, orgDf], axis=0)
                                )
                self.completeWindow()
            except KeyError:
                #正式版
                GoogleSheets().setWorksheet(
                    spreadsheetId='1YWi8hg545ryn_KlWUSp2jDKt85BZ1ACHGw67iwIyrgY',
                    range=self.FILEBRAND ,
                    df = reshapeDf
                )
                #副總版
                GoogleSheets().setWorksheet(
                    spreadsheetId='1eNM9ptlGi4BUKInshOg2dOauso6INK8uWJO-Cz7IGIU',
                    range=self.FILEBRAND ,
                    df = reshapeDf
                )
                print(reshapeDf)
                self.completeWindow()
        except AttributeError:
            tk.messagebox.showinfo(title = '匯入提示', message = '請選擇檔案!')
        except ValueError:
            tk.messagebox.showinfo(title = '匯入提示', message = '資料無法辨識，請至SimsilarWeb網頁下載資料後直接匯入!')
        except Exception as e:
            print(e)
            tk.messagebox.showinfo(title = '匯入提示', message = '執行失敗，請再試一次!')
        
    def curDataDat(self, brand):
        try:
            curDf = GoogleSheets().getWorksheet2Df(
                    spreadsheetId='1YWi8hg545ryn_KlWUSp2jDKt85BZ1ACHGw67iwIyrgY',
                    range=brand)
            curDat = curDf.date[0]
        except IndexError:
            curDat = '目前無資料'
        except:
            curDat = '抓取失敗'
        return curDat
    
    def completeWindow(self):    
        window = tk.Tk()
        window.title('匯入結果')
        window.geometry('310x90')
        
        complete_label = tk.Label(window, text="網頁URL分類完成，已上傳至雲端空間", font=('Arial', 11, 'bold'))
        complete_label.grid(row=0, column=1, padx=10, pady=10,sticky=W+N+E+S,rowspan=2)

        closeWindow = tk.Button(window, text='關閉', font=('Arial', 10), width=10, height=1, command = window.destroy)
        closeWindow.grid(row=2, column=1, padx=10, pady=10,sticky=W,rowspan=2)

        goFile = tk.Button(window, text='前往雲端檔案', font=('Arial', 10), width=10, height=1)
        goFile.bind("<Button-1>", lambda e: self.callback("https://docs.google.com/spreadsheets/d/1eNM9ptlGi4BUKInshOg2dOauso6INK8uWJO-Cz7IGIU/edit#gid=0"))
        goFile.grid(row=2, column=1, padx=10, pady=10,rowspan=5, sticky=E)

        window.mainloop()
        
    def callback(self, url):
        webbrowser.open_new(url)


# In[7]:


root = tk.Tk()
gui = Application(master=root)
gui.mainloop()


# In[ ]:




