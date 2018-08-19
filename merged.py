import gspread
import pandas as pd
import time
import threading
import json
import os
from pathlib import Path
from multiprocessing import Process, Queue
from multiprocessing.pool import ThreadPool
import re
import requests as r
from datetime import datetime as dt
from oauth2client.service_account import ServiceAccountCredentials

scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']

with open("temp", "r") as f:
    offset = int(f.readline())

credentials = ServiceAccountCredentials.from_json_keyfile_name(str(Path.home()) + '/kerala-6debd8a46f6e.json', scope)
def populate(merged, sheet, offset=0):
    cr = "@"
    s = []
    for x in range(26):
        cr = chr(ord(cr) + 1)
        s.append(cr)
    s += ["A" + x for x in s]


    for col, k in zip(list(merged), s):
        print("Pid {} :: pushing column {}".format(str(os.getpid()), col))
        cell_list = sheet.range('{}{}:{}{}'.format(k,1 + offset,k,merged.shape[0]+1+offset))

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
    delete = []
    new_key = []
    for target in maps:
        source = maps[target]
        source = [source] if type(source) == str else source
        new_key.append(target)
        if source[0] == "Type::func":
            df[target] = [globals()[source[1]](x) for x in df[source[2]]]
            delete.append(source[2])
            continue
        delete += source
        df[target] = process(source)
    [delete.pop(key) for key in delete if key in target]
    df.drop(delete, axis=1, inplace=True)
    return df

def dowork():
    try:
        gc = gspread.authorize(credentials)
        merged_sheet = gc.open_by_key("1Xv5uy1_Q8w7y84neYKRuquZwfVVAnE5Qp6PrfM5XbMs").sheet1
        print("PID {} :: starting merge at {} ".format(str(os.getpid()), str(dt.now())))

        with open("config.json", "r") as j:
             sheets = json.load(j)

        mod_list = []
        for k, sheet in enumerate(sheets):
            print("processing sheet {}, sheetId {}".format(k, sheet["sheetId"]))
            if sheet["active"]:
                sht = gc.open_by_key(sheet["sheetId"]).sheet1
                a = pd.DataFrame(sht.get_all_records()).astype(str)
                a = modify(a, sheet["map"])
                mod_list.append(a)
        merged = pd.concat(mod_list).fillna("").sort_values("Date", ascending=False)
        print(merged.shape)
        populate(merged, merged_sheet)
        print("Done!")
    except Exception as e:
        print(str(e))
        raise(e)
        print("merge :: Retry in 5 mins")


def getKeralaSheet():
    global offset
    try:
        print("PID {} :: Starting kerala refresh".format(str(os.getpid())))
        gc = gspread.authorize(credentials)

        # Open a worksheet from spreadsheet with one shot
        sht1 = gc.open_by_key('1BnzyulGK90zLp54Mu2wcP0_2Tpo0cFfEVYBgahdgUic').sheet1
        print("offset {}".format(offset))
        d = pd.DataFrame(r.get("https://keralarescue.in/data?offset={}".format(offset)).json().get("data"))
        d.sort_values("dateadded", ascending=False, inplace=True)
        print("fount {} records in current scan".format(d.shape[0]))
        populate(d, sht1, offset=offset)
        offset = d.shape[0]
        with open("temp", "w") as f:
            f.write(str(offset))
    except Exception as e:
        print(str(e))
        raise(e)
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
