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

credentials = ServiceAccountCredentials.from_json_keyfile_name('kerala-6debd8a46f6e.json', scope)
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


def dowork():
    try:
        gc = gspread.authorize(credentials)
        # Open a worksheet from spreadsheet with one shot
        print("PID {} :: starting merge at {} ".format(str(os.getpid()), str(dt.now())))
        sht1 = gc.open_by_key('1BnzyulGK90zLp54Mu2wcP0_2Tpo0cFfEVYBgahdgUic').sheet1
        sht2 = gc.open_by_key('1Z9qGNFCQHcbVrl_U2fPyTKZkC4nDIpwc3oUk-tiWUDM').sheet1
        sht3 = gc.open_by_key('1qAW_VirkACO_g2VVSIkKRMmZjzh6xUakSm8w-vnDT60').sheet1

        a1 = pd.DataFrame(sht1.get_all_records()).astype(str)
        a2 = pd.DataFrame(sht2.get_all_records()).astype(str)
        a3 = pd.DataFrame(sht3.get_all_records()).astype(str)

        print(a1.columns)

        a1["GPS Location"] = a1["Input Google Map URL"]
        a1["requestee"] = a1['requestee'] + "\n" + a1['requestee_phone']
        a1["Details"] = a1['detailcloth'] + " " + a1['detailfood'] +" " + a1['detailkit_util'] +" " + a1['detailmed'] +" " + a1['detailrescue'] +" " + a1['detailtoilet'] +" " + a1['detailwater']
        a1["needs"] = a1['needcloth'] +" " + a1['needfood'] +" " + a1['needkit_util'] +" " + a1['needmed'] +" " + a1['needothers'] +" " + a1['needrescue'] +" " + a1['needtoilet'] +" " + a1['needwater']
        a1["Date"] = [re.sub("\.[0-9]+Z","",re.sub(r'T'," ",re.sub(r'-', "/", x))) for x in a1["dateadded"]]
        a1["Time of SOS call"] = a1["dateadded"]
        a1.drop(['detailcloth', 'detailfood', 'detailkit_util', 'detailmed', 'detailrescue', 'detailtoilet', 'detailwater'] +
                ['needcloth', 'needfood', 'needkit_util', 'needmed', 'needothers', 'needrescue', 'needtoilet', 'needwater'] +
                ["Input Google Map URL", "dateadded"], axis=1, inplace=True)


        def augTime(a):
            try:
                date_obj = dt.strptime(a, '%d/%m/%Y %H:%M:%S')
                return dt.strftime(date_obj, '%Y/%m/%d %H:%M:%S')
            except:
                try:
                    date_obj = dt.strptime(a, '%m/%d/%Y %H:%M:%S')
                    return dt.strftime(date_obj, '%Y/%m/%d %H:%M:%S')
                except:
                    print(a, "error")
                    return a

        def augTime2(a):
            try:
                date_obj = dt.strptime(a, '%d/%m/%Y %H:%M:%S')
                return dt.strftime(date_obj, '%Y/%m/%d %H:%M:%S')
            except:
                try:
                    date_obj = dt.strptime(a, '%m/%d/%Y %H:%M:%S')
                    return dt.strftime(date_obj, '%Y/%m/%d %H:%M:%S')
                except:
                    print(a, "error")
                    return a

        print(a2.columns)

        a2["GPS Location"] = a2["Google Map Link"]
        a2["Requestee"] = a2['Contact Person & Volunteer comments']
        a2["Details"] = ""
        a2["needs"] = a2['Problem Description']
        a2["Date"]= [augTime(x) for x in a2['Timestamp']]
        a2["Status"] = a2["STATUS"]
        a2["Id"] = a2['R. No']
        a2["Time of SOS call"] = a2["Time of SOS call"]
        a2["LatLong"] = a2["LAt"] + " " + a2["Long"] + " " + a2["LatLong"]
        a2.drop(["STATUS",'R. No',"LAt", "Long", "Google Map Link",'Contact Person & Volunteer comments','Problem Description', 'Timestamp'], axis=1, inplace=True)

        print(a3.columns)
        a3["Date"] = [augTime2(x) for x in a3['Timestamp']]
        a3["Email"] = a3["CONTACT EMAIL"] + " " + a3["CONTACT MOBILE 2"] + " " + a3["CONTACT MOBILE 1"]
        a3["Requestee"] = a3["Address"] + " " \
                        + a3["Informant email id"] + " " \
                        + a3["Additional comments by informant"] + " " \
                        + a3["Informant mobile number"]
        a3["Time of SOS call"] = a3["SOS call time"] + " " + a3["SOS call date"]
        a3["GPS Location"] = a3["MAP LatLong coordinates"]
        a3["Status"] = a3["safe ?"]
        a3["Source"] = a3["SOURCE"]
        a3.drop(["Timestamp", "CONTACT EMAIL", "CONTACT MOBILE 2", "CONTACT MOBILE 1",
                "Address", "Informant email id", "Additional comments by informant", "Informant mobile number",
                "SOS call time", "SOS call date", "MAP LatLong coordinates", "safe ?", "SOURCE"],
                axis=1, inplace=True)

        merged = pd.concat([a1,a2]).fillna("")

        sh = gc.open_by_key("1Xv5uy1_Q8w7y84neYKRuquZwfVVAnE5Qp6PrfM5XbMs")
        merged1 = sh.sheet1

        populate(merged, merged1)
        print("Done!")
    except Exception as e:
        print(str(e))
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
