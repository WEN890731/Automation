from main import GoogleAPIClient
import pandas as pd
import datetime
from datetime import date
import numpy as np
import pymssql 
import pandas as pd
from dateutil.relativedelta import relativedelta


conn = pymssql.connect(server='192.168.8.99',  #主機
                       user='H2022021',   #輸入自己的登入帳戶
                       password='H2022D06gs',  #輸入自己的登入密碼
                       database='SEO_Analysis')  #資料庫名稱
cursor=conn.cursor(as_dict=True) 


# # Function


class GoogleSheets(GoogleAPIClient):
    def __init__(self) -> None:
        # 呼叫 GoogleAPIClient.__init__()，並提供 serviceName, version, scope
        super().__init__(
            'sheets',
            'v4',
            ['https://www.googleapis.com/auth/spreadsheets'],
        )

    def getWorksheet2Df(self, spreadsheetId: str, range: str):
        request = self.googleAPIService.spreadsheets().values().get(
            spreadsheetId=spreadsheetId,
            range=range,
        )
        result = request.execute()['values']
        header = result[0]
        del result[0]
        return pd.DataFrame(result, columns=header)
    
    def getWorksheet(self, spreadsheetId: str, range: str):
        request = self.googleAPIService.spreadsheets().values().get(
            spreadsheetId=spreadsheetId,
            range=range,
        )
        response = request.execute()['values']
        return response

    #把試算表裡原有的數據刪除
    def clearWorksheet(self, spreadsheetId: str, range: str):
        self.googleAPIService.spreadsheets().values().clear(
            spreadsheetId=spreadsheetId,
            range=range,
        ).execute()
        return 'done'
    
    def setWorksheet(self, spreadsheetId: str, range: str, df: pd.DataFrame):
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

    def appendWorksheet(self, spreadsheetId: str, range: str, df: pd.DataFrame):
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
    


def howManyDays(year):
    if year % 4 == 0 and year % 100 != 0:
        return 364
    elif year % 4 == 0 and year % 100 == 0 and year % 400==0:
        return 364
    else:
        return 365

def getCompare3DaysToday():
    yesterday = ((datetime.datetime.now())+datetime.timedelta(days=-1)).strftime('%Y/%m/%d')
    diffday1 = howManyDays(datetime.datetime.now().year-1)
    oneYago = ((datetime.datetime.now())+datetime.timedelta(days=-diffday1)).strftime('%Y/%m/%d')
    diffday2 = diffday1+howManyDays(datetime.datetime.now().year-2)
    twoYago = ((datetime.datetime.now())+datetime.timedelta(days=-diffday2)).strftime('%Y/%m/%d')
    return yesterday, oneYago, twoYago

def setCompare3Days(yyyy, mm, dd):
    day = datetime.date(yyyy, mm, dd)
    day1 = ((day)+datetime.timedelta(days=-1)).strftime('%Y/%m/%d')
    diffday1 = howManyDays(datetime.datetime.now().year-1)
    day2 = ((day)+datetime.timedelta(days=-diffday1)).strftime('%Y/%m/%d')
    diffday2 = diffday1+howManyDays(datetime.datetime.now().year-2)
    day3 = ((day)+datetime.timedelta(days=-diffday2)).strftime('%Y/%m/%d')
    return day1, day2, day3

def getDayDetail(yyyy, mm, dd):
    week = ['一','二','三','四','五','六','日']
    day = datetime.date(yyyy, mm, dd)
    dayFormat = day.strftime('%Y/%m/%d')
    dayWeek = week[day.weekday()]
    dayYear = day.year
    dayMonth = day.month
    return dayYear, dayMonth, dayFormat,dayWeek

def gaDateDf():
        dateDf = pd.DataFrame()
        dateDf['d1'] = ['yesterday']
        diffday1 = howManyDays(datetime.datetime.now().year-1)
        dateDf['d2'] = [str(diffday1)+'daysAgo']
        diffday2 = diffday1+howManyDays(datetime.datetime.now().year-2)
        dateDf['d3'] = [str(diffday2)+'daysAgo']
        return dateDf
        
def reshapeGaData(gaDataDict):
    comArray = []
    for key in gaDataDict.keys():
        df = gaDataDict[key]
        arr = df[df['Segment']=='Mobile Traffic'].drop('Segment', axis = 1).iloc[0].values.tolist()
        arr.extend(df[df['Segment']=='Tablet and Desktop Traffic'].drop('Segment', axis = 1).iloc[0].values)
        comArray.append(arr)
    return comArray

def reshapeGaDataArray(gaArray, gaColNameList, firstDate, secondDate, thirdDate):
    #共有5個指標、2個區隔和6個頁面，columnName長度須為5的list，指標順序為流量、使用者、入站、單次頁數、跳出率，日期格式須設置為YYYY/m/d，供參考！
    d1Date = datetime.datetime.strptime(firstDate, '%Y/%m/%d')
    d2Date = datetime.datetime.strptime(secondDate, '%Y/%m/%d')
    d3Date = datetime.datetime.strptime(thirdDate, '%Y/%m/%d')
    d1weekDay = getDayDetail(d1Date.year, d1Date.month, d1Date.day)[3]
    d2weekDay = getDayDetail(d2Date.year, d2Date.month, d2Date.day)[3]
    d3weekDay = getDayDetail(d3Date.year, d3Date.month, d3Date.day)[3]

    d1DataList = []
    d2DataList = []
    d3DataList = []
    for ind in np.arange(0,17, 3):
        d1DataList.extend(gaArray[ind])
        d2DataList.extend(gaArray[ind+1])
        d3DataList.extend(gaArray[ind+2])
    gaUpdateDf = pd.DataFrame([d1DataList, d2DataList, d3DataList], columns=gaColNameList*12)
    gaUpdateDf.insert(0, 'weekday', [d1weekDay, d2weekDay, d3weekDay])
    gaUpdateDf.insert(0, 'date', [firstDate, secondDate, thirdDate])
    return gaUpdateDf

def get_week_of_month(year, month, day):
    """
    获取指定的某天是某个月的第几周
    周一为一周的开始
    实现思路：就是计算当天在本年的第y周，本月一1号在本年的第x周，然后求差即可。
    因为查阅python的系统库可以得知：

    """

    begin = int(datetime.date(year, month, 1).strftime("%W"))
    end = int(datetime.date(year, month, day).strftime("%W"))

    return end - begin 

def countByAgent(memDataD1, strs):
    lists  = [memDataD1[memDataD1['agentName']==strs].dateFrom2.values.sum() + memDataD1[memDataD1['agentName']==strs].dateFrom4.values.sum(),
                memDataD1[memDataD1['agentName']==strs].dateFrom3.values.sum() + memDataD1[memDataD1['agentName']==strs].dateFrom5.values.sum() + memDataD1[memDataD1['agentName']==strs].dateFrom6.values.sum()+memDataD1[memDataD1['agentName']==strs].dateFrom7.values.sum() + memDataD1[memDataD1['agentName']==strs].dateFrom9.values.sum() + memDataD1[memDataD1['agentName']==strs].dateFrom10.values.sum() + memDataD1[memDataD1['agentName']==strs].dateFrom11.values.sum(),
                memDataD1[memDataD1['agentName']==strs].dateFrom1.values.sum() + memDataD1[memDataD1['agentName']==strs].dateFrom8.values.sum()+memDataD1[memDataD1['agentName']==strs].dateFrom99.values.sum() ,
                np.nan,
                (memDataD1[memDataD1['agentName']==strs].dateFrom2.values.sum() + memDataD1[memDataD1['agentName']==strs].dateFrom4.values.sum())+(memDataD1[memDataD1['agentName']==strs].dateFrom3.values.sum() + memDataD1[memDataD1['agentName']==strs].dateFrom5.values.sum() + memDataD1[memDataD1['agentName']==strs].dateFrom6.values.sum()+memDataD1[memDataD1['agentName']==strs].dateFrom7.values.sum() + memDataD1[memDataD1['agentName']==strs].dateFrom9.values.sum() + memDataD1[memDataD1['agentName']==strs].dateFrom10.values.sum() + memDataD1[memDataD1['agentName']==strs].dateFrom11.values.sum())+(memDataD1[memDataD1['agentName']==strs].dateFrom1.values.sum() + memDataD1[memDataD1['agentName']==strs].dateFrom8.values.sum()+memDataD1[memDataD1['agentName']==strs].dateFrom99.values.sum())]
                

    return lists

def countFromSummary(memSumD1, strs):
    lists  = [memSumD1.loc[strs].dateFrom2+memSumD1.loc[strs].dateFrom4,
            memSumD1.loc[strs].dateFrom3+memSumD1.loc[strs].dateFrom5+memSumD1.loc[strs].dateFrom6+memSumD1.loc[strs].dateFrom7+memSumD1.loc[strs].dateFrom9+memSumD1.loc[strs].dateFrom10+memSumD1.loc[strs].dateFrom11,
            memSumD1.loc[strs].dateFrom1+memSumD1.loc[strs].dateFrom8+memSumD1.loc[strs].dateFrom99,
            np.nan,
            (memSumD1.loc[strs].dateFrom2+memSumD1.loc[strs].dateFrom4)+(memSumD1.loc[strs].dateFrom3+memSumD1.loc[strs].dateFrom5+memSumD1.loc[strs].dateFrom6+memSumD1.loc[strs].dateFrom7+memSumD1.loc[strs].dateFrom9+memSumD1.loc[strs].dateFrom10+memSumD1.loc[strs].dateFrom11)+(memSumD1.loc[strs].dateFrom1+memSumD1.loc[strs].dateFrom8+memSumD1.loc[strs].dateFrom99)]
            

    return lists


def memTmailCalcuFunc(memDataD1, d1):
    
    memDataD1.loc[memDataD1.agentName.str.contains('測評'), 'agentName'] = memDataD1.loc[memDataD1.agentName.str.contains('測評')].agentName.str.replace('測評' , '校園')
    memDataD1.loc[memDataD1.agentName.str.contains('測評'), 'agentDept'] = '校園經營部'
    memDataD1.loc[memDataD1.agentDept.str.contains('事業'), 'agentDept'] = memDataD1.loc[memDataD1.agentDept.str.contains('事業')].agentDept.str.replace('事業' , '經營')
    
    
    memDataD1.loc[memDataD1.agentName.str.contains('兼差打工'), 'agentDept'] = '校園經營部'
    memDataD1.loc[memDataD1.agentName.str.contains('家教網'), 'agentDept'] = '校園經營部'
    memDataD1.loc[memDataD1.agentName.str.contains('落點分析'), 'agentDept'] = '校園經營部'
    memDataD1.loc[memDataD1.agentName.str.contains('Holland'), 'agentDept'] = '校園經營部'
    memDataD1.loc[memDataD1.agentName.str.contains('大學網'), 'agentDept'] = '校園經營部'
    memDataD1.loc[memDataD1.agentName.str.contains('職涯大師'), 'agentDept'] = '校園經營部'
    memDataD1.loc[memDataD1.agentName.str.contains('校園徵才'), 'agentDept'] = '校園經營部'
    memDataD1.loc[memDataD1.agentName.str.contains('新鮮人找工作'), 'agentDept'] = '校園經營部'
    memDataD1.loc[memDataD1.agentName.str.contains('產學平台.含實習.'), 'agentDept'] = '校園經營部'
    
    memDataD1.loc[memDataD1.agentName.str.contains('兼差打工'), 'agentName'] = '校園_專區_兼差打工'
    memDataD1.loc[memDataD1.agentName.str.contains('家教網'), 'agentName'] = '校園_專區_家教網'
    memDataD1.loc[memDataD1.agentName.str.contains('大學網'), 'agentName'] = '校園_專區_大學網'
    memDataD1.loc[memDataD1.agentName.str.contains('落點分析'), 'agentName'] = '校園_專區_落點分析'
    memDataD1.loc[memDataD1.agentName.str.contains('Holland'), 'agentName'] = '校園_專區_Holland'
    memDataD1.loc[memDataD1.agentName.str.contains('職涯大師'), 'agentName'] = '校園_專區_職涯大師'
    memDataD1.loc[memDataD1.agentName.str.contains('校園徵才'), 'agentName'] = '校園_專區_校園徵才'
    memDataD1.loc[memDataD1.agentName.str.contains('新鮮人找工作'), 'agentName'] = '校園_專區_新鮮人找工作'
    memDataD1.loc[memDataD1.agentName.str.contains('產學平台.含實習.'), 'agentName'] = '校園_專區_產學平台(含實習)'

    
    #計算各類小記
    memSumD1 = memDataD1.groupby(['agentDept']).sum()
    
    #判斷是否所需欄位_原始資料
    if len(memDataD1[memDataD1['agentName']=='校園_專區_兼差打工'].dateFrom2)==0:
        insertDf = pd.DataFrame([0, '校園_專區_兼差打工']+[0]*18)
        insertDf['col'] = memDataD1.columns
        insertDf.set_index(['col'], inplace=True)
        memDataD1 = memDataD1.append(insertDf.T, ignore_index=True)

    if len(memDataD1[memDataD1['agentName']=='校園_專區_家教網'].dateFrom2)==0:
        insertDf = pd.DataFrame([0, '校園_專區_家教網']+[0]*18)
        insertDf['col'] = memDataD1.columns
        insertDf.set_index(['col'], inplace=True)
        memDataD1 = memDataD1.append(insertDf.T, ignore_index=True)

    if len(memDataD1[memDataD1['agentName']=='校園_九大職能星'].dateFrom2)==0:
        insertDf = pd.DataFrame([0, '校園_九大職能星']+[0]*18)
        insertDf['col'] = memDataD1.columns
        insertDf.set_index(['col'], inplace=True)
        memDataD1 = memDataD1.append(insertDf.T, ignore_index=True)

    if len(memDataD1[memDataD1['agentName']=='校園_專區_落點分析'].dateFrom2)==0:
        insertDf = pd.DataFrame([0, '校園_專區_落點分析']+[0]*18)
        insertDf['col'] = memDataD1.columns
        insertDf.set_index(['col'], inplace=True)
        memDataD1 = memDataD1.append(insertDf.T, ignore_index=True)

    if len(memDataD1[memDataD1['agentName']=='校園_專區_Holland'].dateFrom2)==0:
        insertDf = pd.DataFrame([0, '校園_專區_Holland']+[0]*18)
        insertDf['col'] = memDataD1.columns
        insertDf.set_index(['col'], inplace=True)
        memDataD1 = memDataD1.append(insertDf.T, ignore_index=True)

    if len(memDataD1[memDataD1['agentName']=='校園_實體校徵'].dateFrom2)==0:
        insertDf = pd.DataFrame([0, '校園_實體校徵']+[0]*18)
        insertDf['col'] = memDataD1.columns
        insertDf.set_index(['col'], inplace=True)
        memDataD1 = memDataD1.append(insertDf.T, ignore_index=True)

    if len(memDataD1[memDataD1['agentName']=='APP_iOS'].dateFrom2)==0:
        insertDf = pd.DataFrame([0, 'APP_iOS']+[0]*18)
        insertDf['col'] = memDataD1.columns
        insertDf.set_index(['col'], inplace=True)
        memDataD1 = memDataD1.append(insertDf.T, ignore_index=True)

    if len(memDataD1[memDataD1['agentName']=='APP_Android'].dateFrom2)==0:
        insertDf = pd.DataFrame([0, 'APP_Android']+[0]*18)
        insertDf['col'] = memDataD1.columns
        insertDf.set_index(['col'], inplace=True)
        memDataD1 = memDataD1.append(insertDf.T, ignore_index=True)

    #判斷是否所需欄位_Summary資料
    if len(memDataD1[memDataD1['agentDept']=='社群經營中心'].dateFrom2)==0:
        memSumD1.loc['社群經營中心'] = [0]*17
    if len(memDataD1[memDataD1['agentDept']=='企劃部'].dateFrom2)==0:
        memSumD1.loc['企劃部'] = [0]*17
    if len(memDataD1[memDataD1['agentDept']=='校園經營部'].dateFrom2)==0:
        memSumD1.loc['校園經營部'] = [0]*17
    if len(memDataD1[memDataD1['agentDept']=='媒體中心'].dateFrom2)==0:
        memSumD1.loc['媒體中心'] = [0]*17



    #數值計算
    #技術中心
    mainWeb = countFromSummary(memSumD1, '主網_PC')
    
    mainMobile = countFromSummary(memSumD1, '主網_M版')

    appIos = [np.nan, np.nan, np.nan, memDataD1[memDataD1['agentName']=='APP_iOS'].dateMain.values.sum(), memDataD1[memDataD1['agentName']=='APP_iOS'].dateMain.values.sum()]
    appAnd = [np.nan, np.nan, np.nan, memDataD1[memDataD1['agentName']=='APP_Android'].dateMain.values.sum(), memDataD1[memDataD1['agentName']=='APP_Android'].dateMain.values.sum()]
    memDf1 = pd.DataFrame()
    memDf1['mainWeb'] = mainWeb
    memDf1['mainMobile'] = mainMobile
    memDf1['appIos'] = appIos
    memDf1['appAnd'] = appAnd
    memDf1['total1'] = memDf1['mainWeb'].fillna(0) + memDf1['mainMobile'].fillna(0) + memDf1['appIos'].fillna(0) + memDf1['appAnd'].fillna(0)

    #數位經營中心
    planAct = countFromSummary(memSumD1, '企劃部')

    planPt = countByAgent(memDataD1, '校園_專區_兼差打工')

    plantotur = countByAgent(memDataD1, '校園_專區_家教網')
                
    test9  = countByAgent(memDataD1, '校園_九大職能星')

    planPoint  = countByAgent(memDataD1, '校園_專區_落點分析')

    planHolland = countByAgent(memDataD1, '校園_專區_Holland')

    planHire = countByAgent(memDataD1, '校園_實體校徵')

    digT = countFromSummary(memSumD1, '校園經營部')
    digTotal = [digT[0]+planAct[0],
               digT[1]+planAct[1],
               digT[2]+planAct[2],
               np.nan,
               digT[4]+planAct[4]]
    
    digOrther = [digTotal[0]-planAct[0]-planPt[0]-plantotur[0]-test9[0]-planPoint[0]-planHolland[0]-planHire[0],
                digTotal[1]-planAct[1]-planPt[1]-plantotur[1]-test9[1]-planPoint[1]-planHolland[1]-planHire[1],
                digTotal[2]-planAct[2]-planPt[2]-plantotur[2]-test9[2]-planPoint[2]-planHolland[2]-planHire[2],
                np.nan,
                digTotal[4]-planAct[4]-planPt[4]-plantotur[4]-test9[4]-planPoint[4]-planHolland[4]-planHire[4]]

    memDf2 = pd.DataFrame()
    memDf2['planAct'] = planAct
    memDf2['planPt'] = planPt
    memDf2['plantotur'] = plantotur
    memDf2['test9'] = test9
    memDf2['planPoint'] = planPoint
    memDf2['planHolland'] = planHolland
    memDf2['planHire'] = planHire
    memDf2['planHolland'] = planHolland
    memDf2['digOrther'] = digOrther
    memDf2['total2'] = digTotal

    #媒體中心
    commu = countFromSummary(memSumD1, '社群經營中心')

    mad = countFromSummary(memSumD1, '媒體中心')
    
    commuTotal = [commu[0] + mad[0],
            commu[1] + mad[1],
            commu[2] + mad[2],
            np.nan,
            commu[4] + mad[4]]

    memDf3 = pd.DataFrame()
    memDf3['commu'] = commu
    memDf3['mad'] = mad
    memDf3['total3'] = commuTotal


    #結果合併
    fal = pd.concat([memDf1, memDf2, memDf3], axis=1)
    fal['total4'] = fal.total1.fillna(0) + fal.total2.fillna(0) + fal.total3.fillna(0)
    mrClass = ['SEO','廣告','自然/其他','APP','合計']
    fal.insert(loc=0, column='Source', value=mrClass)
    dateCol = [d1]*5
    fal.insert(loc=0, column='Date', value=dateCol)
    fal.fillna('-', inplace= True)

    return fal

# def memSourFunc(memDataD1, d1):
#     memSumm = memDataD1.sum()

#     #判斷是否所需欄位_原始資料

#     if len(memDataD1[memDataD1['agentName']=='企劃_專區_落點分析'].dateFrom2)==0:
#         insertDf = pd.DataFrame([0, '企劃_專區_落點分析']+[0]*18)
#         insertDf['col'] = memDataD1.columns
#         insertDf.set_index(['col'], inplace=True)
#         memDataD1 = memDataD1.append(insertDf.T, ignore_index=True)

#     if len(memDataD1[memDataD1['agentName']=='測評_九大職能星'].dateFrom2)==0:
#         insertDf = pd.DataFrame([0, '測評_九大職能星']+[0]*18)
#         insertDf['col'] = memDataD1.columns
#         insertDf.set_index(['col'], inplace=True)
#         memDataD1 = memDataD1.append(insertDf.T, ignore_index=True)

#     if len(memDataD1[memDataD1['agentName']=='APP_iOS'].dateFrom2)==0:
#         insertDf = pd.DataFrame([0, 'APP_iOS']+[0]*18)
#         insertDf['col'] = memDataD1.columns
#         insertDf.set_index(['col'], inplace=True)
#         memDataD1 = memDataD1.append(insertDf.T, ignore_index=True)

#     if len(memDataD1[memDataD1['agentName']=='APP_Android'].dateFrom2)==0:
#         insertDf = pd.DataFrame([0, 'APP_Android']+[0]*18)
#         insertDf['col'] = memDataD1.columns
#         insertDf.set_index(['col'], inplace=True)
#         memDataD1 = memDataD1.append(insertDf.T, ignore_index=True)

#     memFromTotal = [memSumm.dateFrom2, 
#                 memSumm.dateFrom3, 
#                 memSumm.dateMain - memSumm.dateApp - memSumm.dateFrom2 - memSumm.dateFrom3, 
#                 (memSumm.dateFrom2+memSumm.dateFrom3+(memSumm.dateMain - memSumm.dateApp - memSumm.dateFrom2 - memSumm.dateFrom3)),
#                 memSumm.dateApp, 
#                 memSumm.dateMain]
     

#     memFrom9 = [memDataD1[memDataD1['agentName']=='測評_九大職能星'].dateFrom2.values.sum(), 
#                 memDataD1[memDataD1['agentName']=='測評_九大職能星'].dateFrom3.values.sum(), 
#                 memDataD1[memDataD1['agentName']=='測評_九大職能星'].dateMain.values.sum() - (memDataD1[memDataD1['agentName']=='測評_九大職能星'].dateFrom2.values.sum()) - (memDataD1[memDataD1['agentName']=='測評_九大職能星'].dateFrom3.values.sum()), 
#                 memDataD1[memDataD1['agentName']=='測評_九大職能星'].dateMain.values.sum()]
    

#     memFromPoint = [memDataD1[memDataD1['agentName']=='企劃_專區_落點分析'].dateFrom2.values.sum(), 
#                 memDataD1[memDataD1['agentName']=='企劃_專區_落點分析'].dateFrom3.values.sum(), 
#                 memDataD1[memDataD1['agentName']=='企劃_專區_落點分析'].dateMain.values.sum() - (memDataD1[memDataD1['agentName']=='企劃_專區_落點分析'].dateFrom2.values.sum()) - (memDataD1[memDataD1['agentName']=='企劃_專區_落點分析'].dateFrom3.values.sum()), 
#                 memDataD1[memDataD1['agentName']=='企劃_專區_落點分析'].dateMain.values.sum()]
    

#     memFromExc9P = [memFromTotal[0]-memFrom9[0]-memFromPoint[0],
#                     memFromTotal[1]-memFrom9[1]-memFromPoint[1],
#                     memFromTotal[2]-memFrom9[2]-memFromPoint[2],
#                     memFromTotal[3]-memFrom9[3]-memFromPoint[3]]
    

#     memFromApp = [memDataD1[memDataD1['agentName']=='APP_iOS'].dateMain.values.sum(), 
#                 memDataD1[memDataD1['agentName']=='APP_Android'].dateMain.values.sum(), 
#                 (memDataD1[memDataD1['agentName']=='APP_iOS'].dateMain.values.sum()+ memDataD1[memDataD1['agentName']=='APP_Android'].dateMain.values.sum())]
    
#     tabSourList = [d1, memSumm.dateFrom2, 
#                 memSumm.dateFrom3, 
#                 memSumm.dateMain - memSumm.dateApp - memSumm.dateFrom2 - memSumm.dateFrom3, 
#                 (memSumm.dateFrom2+memSumm.dateFrom3+(memSumm.dateMain - memSumm.dateApp - memSumm.dateFrom2 - memSumm.dateFrom3)),
#                 memDataD1[memDataD1['agentName']=='APP_iOS'].dateMain.values.sum(), 
#                 memDataD1[memDataD1['agentName']=='APP_Android'].dateMain.values.sum(), 
#                 (memDataD1[memDataD1['agentName']=='APP_iOS'].dateMain.values.sum()+ memDataD1[memDataD1['agentName']=='APP_Android'].dateMain.values.sum()),
#                 ((memSumm.dateFrom2+memSumm.dateFrom3+(memSumm.dateMain - memSumm.dateApp - memSumm.dateFrom2 - memSumm.dateFrom3)))+((memDataD1[memDataD1['agentName']=='APP_iOS'].dateMain.values.sum()+ memDataD1[memDataD1['agentName']=='APP_Android'].dateMain.values.sum()) )]
                
#     memFromTotal = [str(i) for i in memFromTotal]
#     memFrom9 = [str(i) for i in memFrom9] 
#     memFromPoint = [str(i) for i in memFromPoint] 
#     memFromExc9P = [str(i) for i in memFromExc9P]
#     memFromApp = [str(i) for i in memFromApp] 
#     tabSourList = [str(i) for i in tabSourList] 

#     week = ['一','二','三','四','五','六','日']
#     dayWeek = week[datetime.datetime.strptime(d1, '%Y/%m/%d').weekday()]

#     falDf = pd.DataFrame([d1]+[dayWeek]+memFromTotal+memFrom9+memFromPoint+memFromExc9P+memFromApp)
#     falDf['col'] = ['日期','W','SEO','廣告','其他','小計','APP','合計','SEO','廣告','其他','小計','SEO','廣告','其他','小計','SEO','廣告','其他','小計','ios','Android','小計']
#     falDf = falDf.set_index(['col']).T
#     tabSourDf = pd.DataFrame(tabSourList).T

#     return falDf, tabSourDf

############################################################################################################################## def end

d1, d2, d3 = getCompare3DaysToday()

# # GA Report


#[google analytics]
spreadsheetIdList = ['1EA0fY7FVFRCG2ihC6Ildfigxgt5oeM0NIzbv6kwFQlw', '1WQK-7M3xA2dmSTOjZ94fbb4q3R1peeXCush6HyHv3G0', '1XqLC6SbSHhQ7bN4y_HKE8T1Y_4BHOxlqcG6LPZZqfu4',
                    '1g47eB68biLPC_kpSPw7Pkjf3yzDlZArq3bIEa5SpuT8', '1l1HysPLwGJihgUM4KwtCY8ydkm8Rc0Mab-SuP2-WevE', '1Sz8Ge7XeZGvJhQDHbRZyWreafCdrl5wB6PcRghZODjc']
dfNameList = ['allPage', 'mainPage', 'homePage', 'corpPage', 'empPage', 'resultPage']

sheetNameList = ['summaryD1', 'summaryD2', 'summaryD3']
gaDataDict = dict()

SEOsheetNameList = ['summarySEOD1', 'summarySEOD2', 'summarySEOD3']
gaSEODataDict = dict()

for i in range(len(spreadsheetIdList)):
    for j in range(len(sheetNameList)):
        sheet = GoogleSheets().getWorksheet(
            spreadsheetId=spreadsheetIdList[i],
            range=sheetNameList[j])
        colName = sheet[0]
        df = pd.DataFrame(sheet[1:], columns=colName)
        dictKey = dfNameList[i]+'_'+sheetNameList[j]
        gaDataDict[dictKey] = df

        sheet = GoogleSheets().getWorksheet(
            spreadsheetId=spreadsheetIdList[i],
            range=SEOsheetNameList[j])
        colName = sheet[0]
        df2 = pd.DataFrame(sheet[1:], columns=colName)
        dictKey2 = dfNameList[i]+'_'+SEOsheetNameList[j]
        gaSEODataDict[dictKey2] = df2

#自動化排程執行程式
col = gaDataDict['allPage_summaryD1'].drop(['Segment'], axis = 1).columns.tolist()
gaArray = reshapeGaData(gaDataDict)
gaArraySEO = reshapeGaData(gaSEODataDict)

gaFalDf = reshapeGaDataArray(gaArray=gaArray, gaColNameList=col, firstDate=d1, secondDate=d2, thirdDate=d3)
gaSEOFalDf = reshapeGaDataArray(gaArray=gaArraySEO, gaColNameList=col, firstDate=d1, secondDate=d2, thirdDate=d3)

GoogleSheets().appendWorksheet(
        spreadsheetId='1tOUO7ZCOJSXLaBb_BYRlYfzceOVYpxyRlKDEKyoEHq0',
        range='GA',
        df=gaFalDf
)
GoogleSheets().appendWorksheet(
        spreadsheetId='1tOUO7ZCOJSXLaBb_BYRlYfzceOVYpxyRlKDEKyoEHq0',
        range='GA_SEO',
        df=gaSEOFalDf
)

# Tableau
tGaDf = gaFalDf['流量']
tGaDf.insert(0,column = 'date', value = gaFalDf['date'].values.tolist())
GoogleSheets().appendWorksheet(
        spreadsheetId='1Qc3aF_ukiLzX2wKI1B4U9i86h4wSZPPPi7TqoIarZsY',
        range='Sheet1',
        df=tGaDf
)

# 會員主投流量報表

## IOS

sql = "select coun.dayIn, coun.tmailCount, dist.talentCount, 'ios' as 'agent' from (select Convert(varchar(10),dateIn,111) as dayIn, COUNT(*) as tmailCount from tMailAgentAnalyze2022 where agent like '%ios%' group by Convert(varchar(10),dateIn,111)) as coun inner join (select a.dayIn, COUNT(*)  as talentCount from (select DISTINCT talentNo , Convert(varchar(10),dateIn,111) as dayIn from tMailAgentAnalyze2022 where agent like '%ios%') as a group by a.dayIn) as dist on coun.dayIn = dist.dayIn where coun.dayIn = "+"'%s';" % (d1)
cursor.execute(sql)

result = cursor.fetchall()

iosDelivDf = pd.DataFrame(result)


## Android


sql = '''select coun.dayIn, coun.tmailCount, dist.talentCount, 'android' as 'agent' from (select Convert(varchar(10),dateIn,111) as dayIn, COUNT(*) as tmailCount from tMailAgentAnalyze2022 where agent like '%android%' group by Convert(varchar(10),dateIn,111)) as coun inner join (select a.dayIn, COUNT(*)  as talentCount from (select DISTINCT talentNo , Convert(varchar(10),dateIn,111) as dayIn from tMailAgentAnalyze2022 where agent like '%android%') as a group by a.dayIn) as dist on coun.dayIn = dist.dayIn where coun.dayIn  = '''+"'%s';" % (d1)
cursor.execute(sql)



result = cursor.fetchall()

andDelivDf = pd.DataFrame(result)


# ## Web&Mobile


sql = '''select w.dayIn, w.web, m.mobile , allC.allTalent from (select Convert(varchar(10),dateIn,111) as dayIn, count(distinct(talentNo)) as web from tMailAgentAnalyze2022 where sNo not in (select sNo from tMailAgentAnalyze2022 where agent like '%mobile1_%' escape 1 or agent like '%1_app1_%' escape 1)  group by Convert(varchar(10),dateIn,111)) as w inner join (select Convert(varchar(10),dateIn,111) as dayIn, count(distinct(talentNo)) as mobile from tMailAgentAnalyze2022 where sNo in (select sNo from tMailAgentAnalyze2022  where agent like '%mobile1_%' escape 1 and agent not like '%1_app1_%' escape 1)  group by Convert(varchar(10),dateIn,111)) as m on w.dayIn = m.dayIn inner join  (select Convert(varchar(10),dateIn,111) as dayIn, count(distinct(talentNo)) as allTalent from tMailAgentAnalyze2022 group by Convert(varchar(10),dateIn,111)) as allC on allC.dayIn = m.dayIn where w.dayIn  = '''+"'%s';" % (d1)

#主投、會員人數
cursor.execute(sql)


result = cursor.fetchall()

webMobileDfTalent = pd.DataFrame(result)


sql = "select w.dayIn, w.web, m.mobile , (w.web+ m.mobile)  as allTalent from (select Convert(varchar(10),dateIn,111) as dayIn, count(talentNo) as web from tMailAgentAnalyze2022 where sNo not in (select sNo from tMailAgentAnalyze2022  where agent like '%mobile1_%' escape 1 or agent like '%1_app1_%' escape 1) group by Convert(varchar(10),dateIn,111)) as w inner join (select Convert(varchar(10),dateIn,111) as dayIn, count(talentNo) as mobile from tMailAgentAnalyze2022 where sNo in (select sNo from tMailAgentAnalyze2022  where agent like '%mobile1_%' escape 1 and agent not like '%1_app1_%' escape 1) group by Convert(varchar(10),dateIn,111)) as m on w.dayIn = m.dayIn where w.dayIn  ="+"'%s';" % (d1)
#主投、會員次數
cursor.execute(sql)


result = cursor.fetchall()

webMobileDfCount= pd.DataFrame(result)


# ## 資料處理


delivDataSummList = [int(iosDelivDf.talentCount.values), int(andDelivDf.talentCount.values), int((iosDelivDf.talentCount.values+andDelivDf.talentCount.values)),
                          int(iosDelivDf.tmailCount.values), int(andDelivDf.tmailCount.values), int(iosDelivDf.tmailCount.values+int(andDelivDf.tmailCount.values)),
                      round(int(iosDelivDf.tmailCount.values)/int(iosDelivDf.talentCount.values), 2), round(int(andDelivDf.tmailCount.values)/int(andDelivDf.talentCount.values), 2),
                        ' ', ' ',' ',' ',' ',' ',' ',' ',
                        int(webMobileDfTalent.web.values), int(webMobileDfTalent.mobile.values), 
                        int(webMobileDfTalent.allTalent.values), int(webMobileDfCount.web.values), int(webMobileDfCount.mobile.values), int(webMobileDfCount.allTalent.values), 
                      round(int( webMobileDfCount.web.values)/int(webMobileDfTalent.web.values), 2), round(int(webMobileDfCount.mobile.values)/int(webMobileDfTalent.mobile.values), 2)
                      ]



d1, d2, d3 = getCompare3DaysToday()
d1Date = datetime.datetime.strptime(d1, '%Y/%m/%d')
d1Detail = list(getDayDetail(d1Date.year, d1Date.month, d1Date.day))
d1Detail.extend(delivDataSummList)



#[主投人次]
GoogleSheets().appendWorksheet(
        spreadsheetId='1jbA88J1jYdvbmiF4ZtVTGBp0lxVbbE4ZmVRZx5RQYDY',
        range='主投統計',
        df=pd.DataFrame(d1Detail).T
)

#Tableau
tabTmailDf = pd.DataFrame([d1, int(iosDelivDf.tmailCount.values), int(andDelivDf.tmailCount.values), int(iosDelivDf.tmailCount.values)+int(andDelivDf.tmailCount.values),
                        int(webMobileDfCount.web.values), int(webMobileDfCount.mobile.values), int(webMobileDfCount.allTalent.values)]).T

GoogleSheets().appendWorksheet(
        spreadsheetId='1QZL4W8a2c42OG9_E80KFtaADGe8_FC5YHKO1xTsJYjI',
        range='Sheet1',
        df=tabTmailDf
)

# 履歷更新
cursor.execute('''
/*****************/

IF OBJECT_ID('dbo.temp_Resume_CNT_ALL', 'U') IS NOT NULL 
Drop table temp_Resume_CNT_ALL ;

Select X.dateIn,
       Y.iOS_CNT,
	   Z.Android_CNT,
	   W.Mobile_CNT,
	   U.PC_CNT
into   temp_Resume_CNT_ALL
from (
	Select distinct Convert(varchar(10), dateIn, 111) as dateIn
	from   tResumeEditLog
	where Convert(varchar(10), dateIn, 111) = Convert(varchar(10), getdate()-1, 111)
	Group by dateIn )           X 
left join (
	Select P.dateIn,
	   sum(P.CNT) as iOS_CNT
--into   #temp_Resume_iOS
from (
	Select B.talentNo,
		   B.Source_F,
		   B.dateIn,
		   count(*) CNT
	from (
		Select A.*
		from (
			Select talentNo, 
				   resumeGuid,
				   Convert(varchar(10), dateIn, 111) as dateIn,
				   case when agent like '%app_ios%' then 'iOS'
						when agent like '%app_android%' then 'Android'
						when agent like '%mobile%' escape 1 and agent not like '%app%' escape 1 then 'Mobile'
				   Else 'PC' end as Source_F 
			from   tResumeEditLog
			where  len(resumeGuid) > 7 and Convert(varchar(10), dateIn, 111) = Convert(varchar(10), getdate()-1, 111)
		   )  A
		Group by  A.talentNo, A.resumeGuid, A.dateIn, A.Source_F 
	  )  B
	Group by  B.talentNo, B.Source_F, B.dateIn
  )  P  where  P.Source_F = 'iOS'
group by  P.dateIn
)      Y
on   X.dateIn = Y.dateIn
left join (
	Select P.dateIn,
	   sum(P.CNT) as Android_CNT
--into   #temp_Resume_Android
from (
	Select B.talentNo,
		   B.Source_F,
		   B.dateIn,
		   count(*) CNT
	from (
		Select A.*
		from (
			Select talentNo, 
				   resumeGuid,
				   Convert(varchar(10), dateIn, 111) as dateIn,
				   case when agent like '%app_ios%' then 'iOS'
						when agent like '%app_android%' then 'Android'
						when agent like '%mobile%' escape 1 and agent not like '%app%' escape 1 then 'Mobile'
				   Else 'PC' end as Source_F 
			from   tResumeEditLog
			where  len(resumeGuid) > 7 and Convert(varchar(10), dateIn, 111) = Convert(varchar(10), getdate()-1, 111)
		   )  A
		Group by  A.talentNo, A.resumeGuid, A.dateIn, A.Source_F 
	  )  B
	Group by  B.talentNo, B.Source_F, B.dateIn
  )  P  where  P.Source_F = 'Android'
group by  P.dateIn
)  Z
on   X.dateIn = Z.dateIn
left join (
	Select P.dateIn,
	   sum(P.CNT) as Mobile_CNT
--into   #temp_Resume_Mobile
from (
	Select B.talentNo,
		   B.Source_F,
		   B.dateIn,
		   count(*) CNT
	from (
		Select A.*
		from (
			Select talentNo, 
				   resumeGuid,
				   Convert(varchar(10), dateIn, 111) as dateIn,
				   case when agent like '%app_ios%' then 'iOS'
						when agent like '%app_android%' then 'Android'
						when agent like '%mobile%' escape 1 and agent not like '%app%' escape 1 then 'Mobile'
				   Else 'PC' end as Source_F 
			from   tResumeEditLog
			where  len(resumeGuid) > 7 and Convert(varchar(10), dateIn, 111) = Convert(varchar(10), getdate()-1, 111)
		   )  A
		Group by  A.talentNo, A.resumeGuid, A.dateIn, A.Source_F 
	  )  B
	Group by  B.talentNo, B.Source_F, B.dateIn
  )  P  where  P.Source_F = 'Mobile'
group by  P.dateIn
)   W
on   X.dateIn = W.dateIn
left join (
	Select P.dateIn,
	   sum(P.CNT) as PC_CNT
--into   #temp_Resume_PC
from (
	Select B.talentNo,
		   B.Source_F,
		   B.dateIn,
		   count(*) CNT
	from (
		Select A.*
		from (
			Select talentNo, 
				   resumeGuid,
				   Convert(varchar(10), dateIn, 111) as dateIn,
				   case when agent like '%app_ios%' then 'iOS'
						when agent like '%app_android%' then 'Android'
						when agent like '%mobile%' escape 1 and agent not like '%app%' escape 1 then 'Mobile'
				   Else 'PC' end as Source_F 
			from   tResumeEditLog
			where  len(resumeGuid) > 7 and Convert(varchar(10), dateIn, 111) = Convert(varchar(10), getdate()-1, 111)
		   )  A
		Group by  A.talentNo, A.resumeGuid, A.dateIn, A.Source_F 
	  )  B
	Group by  B.talentNo, B.Source_F, B.dateIn
  )  P  where  P.Source_F = 'PC'
group by  P.dateIn
)       U
on   X.dateIn = U.dateIn;


Select * from temp_Resume_CNT_ALL;
''')

result = cursor.fetchall()

ResumeEditLog= pd.DataFrame(result)

GoogleSheets().appendWorksheet(
        spreadsheetId='1vbqT4oyr1GZpyf6tmM8jfoJcQ1U2sx_hABuJqHwyZuU',
        range='Sheet1',
        df=ResumeEditLog
)
################################################################################################################### 以上基本沒有問題

# 新會員來源及主投分析

## 會員來源細分_NEW
# day1
sql = "select * from tMailAgentGroupMemberResultDetail where dateCount = '%s';" % (d1)
cursor.execute(sql)
result = cursor.fetchall()

memDataD1 = pd.DataFrame(result)

# day2
sql = "select * from tMailAgentGroupMemberResultDetail where dateCount = '%s';" % (d2)
cursor.execute(sql)
result = cursor.fetchall()

memDataD2 = pd.DataFrame(result)

# day3
sql = "select * from tMailAgentGroupMemberResultDetail where dateCount = '%s';" % (d3)
cursor.execute(sql)
result = cursor.fetchall()

memDataD3 = pd.DataFrame(result)

memDf1 = memTmailCalcuFunc(memDataD1, d1)
memDf2 = memTmailCalcuFunc(memDataD2, d2)
memDf3 = memTmailCalcuFunc(memDataD3, d3)

GoogleSheets().appendWorksheet(
        spreadsheetId='1LRj10lg8O4yGgOncH2TnadTV4Gfe8_4Bfw6Nmz0YC38',
        range='會員來源細分_New',
        df=pd.concat([memDf1, memDf2, memDf3])
)

GoogleSheets().appendWorksheet(
        spreadsheetId='1fbD9PTnr-LavM1unCw57h4mwEvhcjsZtgbYoAAtrYVw',
        range='會員來源_細',
        df=pd.concat([memDf1, memDf2, memDf3])[['Date', 'Source', 'total4']]
)

## 會員來源
# memSourDf1, memSourDf1Tab  = memSourFunc(memDataD1, d1)
# memSourDf2, memSourDf2Tab  = memSourFunc(memDataD2, d2)
# memSourDf3, memSourDf3Tab  = memSourFunc(memDataD3, d3)
# GoogleSheets().appendWorksheet(
#         spreadsheetId='1LRj10lg8O4yGgOncH2TnadTV4Gfe8_4Bfw6Nmz0YC38',
#         range='會員來源',
#         df = pd.concat([memSourDf1, memSourDf2, memSourDf3])
# )

# memSourDf1, memSourDf1Tab  = memSourFunc(memDataD1, d1)
# GoogleSheets().appendWorksheet(
#         spreadsheetId='1EZYOOe0S05uGsvshnJBcMPjSJe-Is9WAZzHQIVH1BL8',
#         range='Sheet1',
#         df = memSourDf1Tab
# )

## 主投來源細分_NEW
# day1
sql = "select * from tMailAgentGroupResultDetail where dateCount = '%s';" % (d1)
cursor.execute(sql)
result = cursor.fetchall()
mailDataD1 = pd.DataFrame(result)

# day2
sql = "select * from tMailAgentGroupResultDetail where dateCount = '%s';" % (d2)
cursor.execute(sql)
result = cursor.fetchall()
mailDataD2 = pd.DataFrame(result)

# day3
sql = "select * from tMailAgentGroupResultDetail where dateCount = '%s';" % (d3)
cursor.execute(sql)
result = cursor.fetchall()
mailDataD3 = pd.DataFrame(result)

mailDf1 = memTmailCalcuFunc(mailDataD1, d1)
mailDf2 = memTmailCalcuFunc(mailDataD2, d2)
mailDf3 = memTmailCalcuFunc(mailDataD3, d3)

GoogleSheets().appendWorksheet(
        spreadsheetId='1LRj10lg8O4yGgOncH2TnadTV4Gfe8_4Bfw6Nmz0YC38',
        range='主投來源細分_New',
        df=pd.concat([mailDf1, mailDf2, mailDf3])
)
GoogleSheets().appendWorksheet(
        spreadsheetId='1fbD9PTnr-LavM1unCw57h4mwEvhcjsZtgbYoAAtrYVw',
        range='主投來源_細',
        df=pd.concat([mailDf1, mailDf2, mailDf3])[['Date', 'Source', 'total4']]
)

# SEO每日數據

date1 = datetime.datetime.strptime(d1, '%Y/%m/%d')

#上周同期
delta = datetime.timedelta(days = 7)
dateLastWeek = datetime.datetime.strftime(date1-relativedelta(days=7), '%Y/%m/%d')
sql = "select * from tMailAgentGroupResultDetail where dateCount = '%s';" % (dateLastWeek)
cursor.execute(sql)
result = cursor.fetchall()
mailDataD4 = pd.DataFrame(result)

#上個月同期
if date1.month==1:
            dateMon = datetime.date(date1.year-1, 12 , 1)+relativedelta(weeks = get_week_of_month(date1.year, date1.month, date1.day))
            dateLastMon = datetime.datetime.strftime(dateMon-relativedelta(days=dateMon.weekday())+relativedelta(days=date1.weekday()), '%Y/%m/%d')
else:
    dateMon = datetime.date(date1.year, date1.month-1 , 1)+relativedelta(weeks = get_week_of_month(date1.year, date1.month, date1.day))
    dateLastMon = datetime.datetime.strftime(dateMon-relativedelta(days=dateMon.weekday())+relativedelta(days=date1.weekday()), '%Y/%m/%d')
            
sql = "select * from tMailAgentGroupResultDetail where dateCount = '%s';" % (dateLastMon)
cursor.execute(sql)
result = cursor.fetchall()
mailDataD5 = pd.DataFrame(result)


mailDf4 = memTmailCalcuFunc(mailDataD4, dateLastWeek)
mailDf5 = memTmailCalcuFunc(mailDataD5, dateLastMon)

seoDf = pd.DataFrame()
seoDf['SEO'] = [d1,dateLastWeek, dateLastMon, d2]

seoYes = mailDf1.total4[0]
seoLastWeek = mailDf4.total4[0]
seoLastMonth = mailDf5.total4[0]
seoLastY = mailDf2.total4[0]

seoDf['主投數'] = [int(seoYes), int(seoLastWeek), int(seoLastMonth), int(seoLastY)]

seoDf['變動率'] = ['X', 
                '{:.2f}'.format((seoYes-seoLastWeek)/seoLastWeek*100)+'%',
                '{:.2f}'.format((seoYes-seoLastMonth)/seoLastMonth*100)+'%',
                '{:.2f}'.format((seoYes-seoLastY)/seoLastY*100)+'%']

seoDf['差異'] = ['X', 
               int(seoYes-seoLastWeek),
                int(seoYes-seoLastMonth),
                int(seoYes-seoLastY)]


GoogleSheets().setWorksheet(
        spreadsheetId='1LRj10lg8O4yGgOncH2TnadTV4Gfe8_4Bfw6Nmz0YC38',
        range='SEO每日數據',
        df=seoDf
)