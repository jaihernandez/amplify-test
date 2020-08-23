import requests
import json
import mysql
import mysql.connector
import pickle


# creating connection 
def create_connection(DataBase):
  mydb = mysql.connector.connect(
      host="mysql-freetier.c8pqcmc8uxzg.us-east-1.rds.amazonaws.com",
      user="Investidea2020",
      passwd="TDWchamps1618",
      database=DataBase,
      #Need to read blobs
      use_pure = True
    )
  return mydb 


# Cash Flow information
def CashFlow(ticker):
    token = "pk_b363f46b9c744471a5a0e1067e17ff88"
    url = "https://cloud.iexapis.com/stable/stock/"+ticker+"/cash-flow?period=annual&token="+token
    response = requests.get(url)
    data = json.loads(response.text)
    data = data['cashflow'][0]
    data = {
        "cashFlow": data["cashFlow"],
        "totalInvestingCashFlows": data["totalInvestingCashFlows"],
        "cashFlowFinancing": data["cashFlowFinancing"],
        "cashChange": data["cashChange"],
        "freeCashFlow": data["cashFlow"]+data["capitalExpenditures"]         
        }
    return data


# Balance Sheet information
def BalanceSheet(ticker):
    token = "pk_b363f46b9c744471a5a0e1067e17ff88"
    url = "https://cloud.iexapis.com/stable/stock/"+ticker+"/balance-sheet?period=annual&token="+token
    response = requests.get(url)
    data = json.loads(response.text)
    data = {
                'currentRatio': data["currentAssets"]/data["totalCurrentLiabilities"],
                'totalAssets': data["totalAssets"],         #####
                'totalEquity': data["shareholderEquity"]    #####
        }
    return data



def keyStats(ticker):
    token = "pk_b363f46b9c744471a5a0e1067e17ff88"
    url = "https://cloud.iexapis.com/stable/stock/"+ticker+"/stats?token="+token
    response = requests.get(url)
    data = json.loads(response.text)
    data = {
                'ttmEPS': data['ttmEPS'],
                'ttmDividendRate': data['ttmDividendRate'],
                'nextEarningsDate': data['nextEarningsDate']        
        }
    return data


def incomeStatement(ticker):
    token = "pk_b363f46b9c744471a5a0e1067e17ff88"
    url = "https://cloud.iexapis.com/stable/stock/"+ticker+"/income?period=annual&token="+token
    response = requests.get(url)
    data = json.loads(response.text)['income'][0]
    data = {
                'ebit': data['ebit']      
        }
    return data

def financials(ticker):
    token = "pk_b363f46b9c744471a5a0e1067e17ff88"
    url = "https://cloud.iexapis.com/stable/stock/"+ticker+"/financials?period=annual&token="+token
    response = requests.get(url)
    data = json.loads(response.text)
    data = data['financials'][0]
    IS = {
            'totalRevenue': data['totalRevenue'],
            'grossProfit': data['grossProfit'],
            'operatingIncome': data['operatingIncome'],
            'netIncome': data['netIncome'],
            'operatingMargin': (data['operatingIncome']/data['totalRevenue'])*100,
            'grossProfitMargin': (data['grossProfit']/data['totalRevenue'])*100,
        }
    BS = {
            "totalCash": data["totalCash"],
            "totalDebt": data["totalDebt"]
        }
    return IS, BS



# calling different IEX APIs to calculate all the "important stats" for a company
def stats(ticker):
    token = "pk_b363f46b9c744471a5a0e1067e17ff88"
    url1 = "https://cloud.iexapis.com/stable/stock/"+ticker+"/advanced-stats?token="+token
    response1 = requests.get(url1)
    data1 = json.loads(response1.text)

    url2 = "https://cloud.iexapis.com/stable/stock/"+ticker+"/cash-flow?period=annual&token="+token
    response2 = requests.get(url2)
    data2 = json.loads(response2.text)
    data2 = data2['cashflow'][0]
    
    url3 = "https://cloud.iexapis.com/stable/stock/"+ticker+"/balance-sheet?token="+token
    response3 = requests.get(url3)
    data3 = json.loads(response3.text)
    data3 = data3['balancesheet'][0]
    
    url4 = "https://cloud.iexapis.com/stable/stock/"+ticker+"/stats?token="+token
    response4 = requests.get(url4)
    data4 = json.loads(response4.text)
    
    url5 = "https://cloud.iexapis.com/stable/stock/"+ticker+"/financials?period=annual&token="+token
    response5 = requests.get(url5)
    data5 = json.loads(response5.text)
    data5 = data5['financials'][0]
    
    url6 = "https://cloud.iexapis.com/stable/stock/"+ticker+"/income?period=annual&token="+token
    response6 = requests.get(url6)
    data6 = json.loads(response6.text)
    data6 = data6['income'][0]
    
    
    data = {
            'sharesFloat': data1['float'],
            'avg10Volume': data1['avg10Volume'],
            'avg30Volume': data1['avg30Volume'],
            'ttmEPS': data1['ttmEPS'],
            'sharesOutstanding': data1['sharesOutstanding'],
            'nextDividendDate': data1['nextDividendDate'],
            'dividendYield': data1['dividendYield'],
            'nextEarningsDate': data1['nextEarningsDate'],
            'exDividendDate': data1['exDividendDate'],
            'beta': data1['beta'],
            'EBITDA': data1['EBITDA'],                               
            'debtToEquity': data1['debtToEquity'],                 
            'profitMargin': None,                          
            'enterpriseValue': data1['enterpriseValue'],
            'enterpriseValueToRevenue': data1['enterpriseValueToRevenue'],
            'priceToSales': data1['priceToSales'],
            'priceToBook': data1['priceToBook'],
            'forwardPERatio': data1['forwardPERatio'],
            'pegRatio': data1['pegRatio'],
            'peHigh': data1['peHigh'], #52 week
            'peLow': data1['peLow'], #52 week
            "cashFlow": data2["cashFlow"],
            "totalInvestingCashFlows": data2["totalInvestingCashFlows"],
            "cashFlowFinancing": data2["cashFlowFinancing"],
            "cashChange": data2["cashChange"],
            "freeCashFlow": None,       
            'currentRatio': None,
            'ttmDividendRate': data4['ttmDividendRate'],      
            'totalRevenue': data5['totalRevenue'],
            'grossProfit': data5['grossProfit'],
            'operatingIncome': data5['operatingIncome'],
            'netIncome': data5['netIncome'],
            'operatingMargin': None,
            'grossProfitMargin': None,
            "totalCash": data5["totalCash"],
            "totalDebt": data5["totalDebt"],
            "totalCashPerShare":  None,
            "cashFlowPerShare": None,
            'revenuePerShare': None,
            'FCFtoEV': None,
            'EVtoEBITDA': None,
            'ROA': None,
            'ROE': None,
            'putCallRatio': data1["putCallRatio"],
            'EBIT': data6["ebit"],
            'EVtoEBIT': None,
            'totalEquity': data3['shareholderEquity'],
            'totalAssets': data3['totalAssets']
        }
    try:
        data['profitMargin'] = data1['profitMargin']*100
    except:
        data['profitMargin'] = None
    try:
        data['operatingMargin'] = (data5['operatingIncome']/data5['totalRevenue'])*100
    except:
        data['operatingMargin'] = None
    try:
        data['EVtoEBITDA'] = (data1['enterpriseValue']/data1['EBITDA'])
    except:
        data['EVtoEBITDA'] = None
    try:
        data['grossProfitMargin'] = (data5['grossProfit']/data5['totalRevenue'])*100
    except:    
        data['grossProfitMargin'] = None
    try:
        data['FCFtoEV'] = ((data2["cashFlow"]+data2["capitalExpenditures"])/data1['enterpriseValue'])*100
    except:
        data['FCFtoEV'] = None
    try:
        data["freeCashFlow"] = data2["cashFlow"]+data2["capitalExpenditures"]
    except:
        data["freeCashFlow"] = None
    try:
        data['currentRatio'] = data3["currentAssets"]/data3["totalCurrentLiabilities"]
    except:
        data['currentRatio'] = None
    try:
        data['totalCashPerShare'] = data5["totalCash"]/data1['sharesOutstanding']
    except:
        data['totalCashPerShare'] = None
    try:
        data["cashFlowPerShare"] = data2["cashFlow"]/data1['sharesOutstanding']
    except:
        data["cashFlowPerShare"] = None
    try:
        data['revenuePerShare'] = data5['totalRevenue']/data1['sharesOutstanding']
    except:
        data['revenuePerShare'] = None
    try:
        data['ROA'] = data3['totalAssets']/data5['netIncome']
    except:
        data['ROA'] = None
    try:
        data['ROE'] = data3['shareholderEquity']/data5['netIncome']
    except:
        data['ROE'] = None


        
    return data
    
# updating data into MySQL tables in a bulk manner
def UpdateBulk(var, table, ID, operation):
    com = '= %s,'
    
    if (operation == "update"):
    
        sql= "UPDATE "+table+" SET"
        for i in var:
            sql = sql+" "+i+" "+com
            
        sql = sql[:-1]
        sql = sql+" WHERE "+ID+" = %s"
        

    elif (operation == "add"):
        
        sql= "INSERT INTO "+table+" ("
        fields = var + [ID]
        
        for i in fields:
            sql = sql +i + ", "
            
        sql = sql[:-2] 
        sql = sql+") Values ("
        sql = sql + ("%s, " * len(fields))
        sql = sql[:-2]  + ')'

    return sql    



def news(ticker, number):
    token = "pk_b363f46b9c744471a5a0e1067e17ff88"
    url = "https://cloud.iexapis.com/stable/stock/"+ticker+"/news/last/"+str(number)+"?token="+token
    response = requests.get(url)
    data = json.loads(response.text)
    urls = []
    for i in range(0,len(data)):
        urls.append(data[i]['url'])
    return urls

def analystRatings(ticker):
    token = "pk_b363f46b9c744471a5a0e1067e17ff88"
    url = "https://cloud.iexapis.com/stable/stock/"+ticker+"/recommendation-trends?token="+token
    response = requests.get(url)
    data = json.loads(response.text)
    
    raiting = 0
    for i in range(len(data)):
        raiting = data[i]['ratingScaleMark'] + raiting
    try:  
        avg = raiting/len(data)
    except:
        avg = None
        print('sdaf')    

    return avg


def companyInfo(ticker):
    token = "pk_b363f46b9c744471a5a0e1067e17ff88"
    url = "https://cloud.iexapis.com/stable/stock/"+ticker+"/company?token="+token
    response = requests.get(url)
    data = json.loads(response.text)
    info = {
        'companyName': data['companyName'],
        'industry': data['industry'],
        'sector': data['sector'],
        'exchange': data['exchange'],
        'companyInfo': pickle.dumps(data['description'])
        }
    return info

# quote: inforamtion of the current trading day and 
def quote(ticker):
    token = "pk_b363f46b9c744471a5a0e1067e17ff88"
    url = "https://cloud.iexapis.com/stable/stock/"+ticker+"/quote?token="+token
    response = requests.get(url)
    data = json.loads(response.text)

    data = {
                'avgTotalVolume': data['avgTotalVolume'],
                'marketCap': data['marketCap'],
                'peRatio': data['peRatio'],
                'week52High': data['week52High'],
                'week52Low': data['week52Low'],
                'ytdChange': data['ytdChange'],
            }

    return data






# tickers = ["A","AA","AAL"]


# tickers = ["AAN"]
# operation = "add"

def handler(event, context):
    
    operation = event['operation']
    tickers = event['tickers']

    # if there are tickers to add, it will be done here
    if len(tickers) != 0:
           
        # Creates db connection
        mydb = create_connection("StockData")
        mycursor = mydb.cursor()
    
        
        # the list that will contain all the data
        data = []
        for ticker in tickers:
            # getting the data from the API and functions
            STATS = stats(ticker)
            quoteStuff = quote(ticker)
            cInfo = companyInfo(ticker)
            a_araiting = analystRatings(ticker)
            news_links = pickle.dumps(news(ticker,5))
        
            # putting the data together
            info = list(STATS.values())+list(quoteStuff.values())+list(cInfo.values())+[news_links] \
            +[a_araiting]+[ticker] 
            # converting data list to a tuple and appending it to the bulk data list
            data.append(tuple(info))
        
        
        # getting the names of the variables so that the data can be placed in the correct location
        variables = list(STATS.keys())+list(quoteStuff.keys())+list(cInfo.keys())+['news']+['avg_araiting']
        
        # building the mySQL text so that the data can be put in the table in a bulk manner
        sql = UpdateBulk(variables, "stockStats","ticker", operation)
        # putting the data in the table
        mycursor.executemany(sql, data)

        # closing access to the database
        mydb.commit()
        mycursor.close()
        mydb.close()
