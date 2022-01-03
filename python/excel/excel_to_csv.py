import os
import re
import sys
import time
from multiprocessing.context import Process

import pandas

### 之前在为了将Excel转换为CSV导入到Hive中, 写的脚本.
### 由于业务提供的数据有点问题.就在代码里面预先作了一些简单的ETL操作.
### 转换完,只要简单load进hive即可(预先在hive中建立表结构).

def trans(filepath):
    df = pandas.read_excel(filepath, header=None, sheet_name=None)
    sheets = df.keys()
    del df
    for sheet in sheets:
        df = pandas.read_excel(filepath, header=None, sheet_name=sheet)
        cols = df.shape[1]
        # 限制列数
        cols = cols if (cols < 72) else 72
        for num in range(cols - 1):
            print(num)
            df[num] = df[num].map(instead_nan).map(lambda x: str(x).replace("\n", "").strip()) \
                .replace("\t", "")\
                .map(lambda x: re.sub(r'\s$', '', x))
            # print("输出列标题", df.columns.values)
            # print("输出值\n", df[1])
        save_path = os.path.dirname(filepath) + "/csv/"
        if not os.path.exists(save_path):
            os.mkdir(save_path)
        df.to_csv(save_path + os.path.basename(filepath) + "." + str(sheet) + ".csv", header=None,
                  sep='\001', index=False, line_terminator="")
    print("done")


def instead_nan(x):
    if pandas.isnull(x):
        return ""
    return x


def trans_company_info():
    filepath = r"C:\Users\Wise\Downloads\csv\分公司人员信息表2021-7-9.xlsx"
    target_path = r"./3.csv"

    df = pandas.read_excel(filepath, sheet_name='分公司', header=None)
    for num in range(71):
        print("转换第" + str(num) + "列", end=",")
        df[num] = df[num].map(instead_nan).map(lambda x: str(x).replace("\n", ""))
    print("转换完成，正在保存为CSV...")
    df.to_csv(target_path, header=None, sep=',', index=False, line_terminator="")


def trans_dir(dirs, func):

    print(dirs)
    files = []
    for dir in dirs:
        dir = str(dir)
        fs = os.listdir(dir)
        for f in fs:
            print(f)
            filepath = ""
            if f.startswith("~$") or not f.endswith(".xlsx"):
                continue
            if dir.endswith("\\"):
                filepath = dir + f
            else:
                filepath = dir + "\\" + f
            files.append(filepath)

    print(files)
    ps = []
    for file in files:
        p = Process(target=trans, args=(str(file),))
        p.start()
        ps.append(p)
        if len(ps) == 2:
            ps[0].join()
            ps[1].join()
            del ps[0]
            del ps[0]


def add_col(file_path, col_name, col_value):
    df = pandas.read_csv(file_path, sep="\t")
    df[col_name] = col_value
    df.to_csv(file_path, sep="\t", index=False, line_terminator="")
    print("ok")


if __name__ == '__main__':
	# 将path改为所在文件的目录
	# 生成后会在该目录生成csv文件夹
    path = r"C:\Users\Wise\Desktop\December\11月薪资数据表 (1)\11月薪资数据表"
    trans_dir((path,), trans)


