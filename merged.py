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

credentials = ServiceAccountCredentials.from_json_keyfile_name(str(Path.home()) + '/kerala-6debd8a46f6e.json', scope)
def populate(merged, sheet, offset=0, original=True):
    cr = "@"
    s = []
    for x in range(26):
        cr = chr(ord(cr) + 1)
        s.append(cr)
    s += ["A" + x for x in s]
    if offset == 0 and original:
        populate(pd.DataFrame([list(merged.columns)], columns=list(merged.columns)), sheet, offset=-1, original=False)

    print("Started pushing data...")
    for col, k in zip(list(merged), s):
        print("\rPid {} :: pushing column {}".format(str(os.getpid()), col))
        # print('populating {}{}:{}{}'.format(k,2 + offset,k,merged.shape[0]+1+offset))
        cell_list = sheet.range('{}{}:{}{}'.format(k,2 + offset,k,merged.shape[0]+1+offset))


        # Update values
        for cell, value in zip(cell_list,list(merged[col])):
            cell.value = value

        # Send update in batch mode
        sheet.update_cells(cell_list)
    print("Done pushing...")

def augTime1(x):
    return re.sub("\.[0-9]+Z","",re.sub(r'T'," ",re.sub(r'-', "/", x)))

def emptycolumn(x):
    return ""

def defaultFileName(x):
    return x if len(x)==0 else "KeralaRescue"

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
    for_delete = [key for key in delete if key in new_key]
    [delete.pop(delete.index(x)) for x in for_delete]
    # print(delete, new_key, for_delete)
    # print("deleted {} items".format(len(for_delete)))
    df = df.drop(delete, axis=1)
    # print(df.columns)
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
                print(a.columns)
                a = modify(a, sheet["map"])
                mod_list.append(a)
        merged = pd.concat(mod_list).fillna("").sort_values("Date", ascending=False)
        merged["FILE NAME"].fillna("keralarescue.in")
        populate(merged, merged_sheet, original=True)
        print("Done!")
    except Exception as e:
        print(str(e))
        raise(e)
        print("MERGE :: Retry in 5 mins")

# gc = gspread.authorize(credentials)
# sht1 = gc.open_by_key('1BnzyulGK90zLp54Mu2wcP0_2Tpo0cFfEVYBgahdgUic').sheet1
def getKeralaSheet():
    global offset
    try:
        with open("temp", "r") as f:
            offset = int(f.readline())
        print("PID {} :: Starting kerala refresh".format(str(os.getpid())))
        gc = gspread.authorize(credentials)
        sht1 = gc.open_by_key('1BnzyulGK90zLp54Mu2wcP0_2Tpo0cFfEVYBgahdgUic').sheet1
        print("offset {}".format(offset))
        d = pd.DataFrame(r.get("https://keralarescue.in/data?offset={}".format(offset)).json().get("data"))
        # d.sort_values("dateadded", inplace=True)
        print("fount {} records in current scan".format(d.shape[0]))
        populate(d, sht1, offset=offset)
        offset += d.shape[0]
        with open("temp", "w") as f:
            f.write(str(offset))
        print("Done!")
    except Exception as e:
        print(str(e))
        raise(e)
        print("kerala data pull :: Retry in 5 mins")

def callfunc():
    try:
        # queue = Queue()
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
