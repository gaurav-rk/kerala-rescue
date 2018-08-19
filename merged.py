import gspread
import pandas as pd
import time
import threading
import os
from multiprocessing import Process, Queue
from multiprocessing.pool import ThreadPool
import re
import requests as r
from datetime import datetime as dt
from oauth2client.service_account import ServiceAccountCredentials

scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']

credentials = ServiceAccountCredentials.from_json_keyfile_name('/Users/gmt9/kerala-6debd8a46f6e.json', scope)
def populate(merged, sheet):
    cr = "@"
    s = []
    for x in range(26):
        cr = chr(ord(cr) + 1)
        s.append(cr)
    s += ["A" + x for x in s]


    for col, k in zip(list(merged), s):
        print("Pid {} :: pushing column {}".format(str(os.getpid()), col))
        cell_list = sheet.range('{}1:{}{}'.format(k,k,merged.shape[0]+1))

        # Update values
        for cell, value in zip(cell_list,[col] + list(merged[col])):
            cell.value = value

        # Send update in batch mode
        sheet.update_cells(cell_list)

def augTime1(x):
    return re.sub("\.[0-9]+Z","",re.sub(r'T'," ",re.sub(r'-', "/", x)))

def augTime(a):
    try:
        date_obj = dt.strptime(a, '%d/%m/%Y %H:%M:%S')
        return dt.strftime(date_obj, '%Y/%m/%d %H:%M:%S')
    except:
        pass
    try:
        date_obj = dt.strptime(a, '%m/%d/%Y %H:%M:%S')
        return dt.strftime(date_obj, '%Y/%m/%d %H:%M:%S')
    except:
        pass
    try:
        date_obj = dt.strptime(a, '%m/%d/%Y %H:%M')
        return dt.strftime(date_obj, '%Y/%m/%d %H:%M')
    except:
        print(a, "error")
        return a

def modify(df, maps):
    def process(cols):
        if cols[0] == "Date":
            return
        return [", ".join(x) for x in df[cols].values.tolist()]

    print(df.columns)
    delete = []
    new_key = []
    for map in maps:
        target, source = map
        source = [source] if type(source) == str else source
        delete += source
        new_key.append(target)
        if len(source) == df.shape[0]:
            df[target] = source
            continue
        df[target] = process(source)
    delete = [delete.pop(key) for key in delete if key in target]
    df.drop(delete, axis=1, inplace=True)
    return df

def dowork():
    try:
        gc = gspread.authorize(credentials)
        # Open a worksheet from spreadsheet with one shot
        print("PID {} :: starting merge at {} ".format(str(os.getpid()), str(dt.now())))

        sht1 = gc.open_by_key('1BnzyulGK90zLp54Mu2wcP0_2Tpo0cFfEVYBgahdgUic').sheet1
        sht2 = gc.open_by_key('1Z9qGNFCQHcbVrl_U2fPyTKZkC4nDIpwc3oUk-tiWUDM').sheet1
        sht3 = gc.open_by_key('1qAW_VirkACO_g2VVSIkKRMmZjzh6xUakSm8w-vnDT60').sheet1
        merged_sheet = gc.open_by_key("1Xv5uy1_Q8w7y84neYKRuquZwfVVAnE5Qp6PrfM5XbMs").sheet1

        a1 = pd.DataFrame(sht1.get_all_records()).astype(str)
        a2 = pd.DataFrame(sht2.get_all_records()).astype(str)
        a3 = pd.DataFrame(sht3.get_all_records()).astype(str)

        print("sizes: \n a1 {}\n a2{} \n a3 {}".format(a1.shape, a2.shape, a3.shape))

        a1 = modify(a1, {
            "GPS Location": "Input Google Map URL",
            "requestee": ["requestee", "requestee_phone"],
            "Details": ['detailcloth','detailfood','detailkit_util','detailmed','detailrescue','detailtoilet','detailwater'],
            "needs": ['needcloth','needfood','needkit_util','needmed','needothers','needrescue','needtoilet','needwater'],
            "Date": [augTime1(x) for x in a1["dateadded"]],
            "Time of SOS call": "dateadded"
        })

        a2 = modify(a2, {
            "GPS Location": "Google Map Link",
            "requestee": "Contact Person & Volunteer comments",
            "Details": ["" for x in a2.index],
            "needs": 'Problem Description',
            "Date": [augTime(x) for x in a2['Timestamp - DNT']],
            "Time of SOS call": "dateadded",
            "Status": "STATUS",
            "Id": 'R. No',
            "LatLong": ["LAt", "Long", "LatLong"]
        })

        a3 = modify(a3, {
            "Date": [augTime(x) for x in a3['Timestamp']],
            "Email": ["CONTACT EMAIL","CONTACT MOBILE 2","CONTACT MOBILE 1"],
            "Requestee": ["Address","Informant email id","Additional comments by informant","Informant mobile number"],
            "Time of SOS call": ["SOS call time","SOS call date"],
            "GPS Location": "MAP LatLong coordinates",
            "Status": "STATUS",
            "Source": "SOURCE"
        })
        merged = pd.concat([a1,a2,a3]).fillna("")
        populate(merged, merged_sheet)
        print("Done!")
    except Exception as e:
        print(str(e))
        raise(e)
        print("merge :: Retry in 5 mins")


def getKeralaSheet():
    try:
        print("PID {} :: Starting kerala refresh".format(str(os.getpid())))
        gc = gspread.authorize(credentials)

        # Open a worksheet from spreadsheet with one shot
        sht1 = gc.open_by_key('1BnzyulGK90zLp54Mu2wcP0_2Tpo0cFfEVYBgahdgUic').sheet1
        d = pd.DataFrame(r.get("https://keralarescue.in/data").json())
        populate(d, sht1)
    except Exception as e:
        print(str(e))
        print("kerala data pull :: Retry in 5 mins")

def callfunc():
    try:
        queue = Queue()
        p1 = Process(target=dowork)
        p2 = Process(target=getKeralaSheet)
        p1.start()
        p2.start()
        p1.join()
        p2.join()
    except Exception as e:
        pass
    threading.Timer(300, callfunc).start()


if __name__ == "__main__":
    callfunc()
