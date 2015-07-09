#!/usr/bin/env python 
# coding=utf-8

import ConfigParser
import sys
import re
import os
import random
import time
import urllib
import errno
import string
import json

global null
#替换文件中的null，防止词典载入错误
null = ""

note_menu = {'1':'获取宏观年度数据', 
                            '2':'获取各省份年度数据',
                             '3':'获取各省份季度数据',
                             '4':'获取各省份月度数据'}

data_dirs     = {'1':'hgnd', 
                            '2':'fsnd',
                             '3':'fsjd',
                             '4':'fsyd'}


def load_dict(tdfile,key_col,value_col_list):
    """
    根据文件和列来构造dict数据结构
    tdfile: 纯文本 表格样式的文件,列之间用"\t"分割 
    key_col:key列号,从0开始
    value_col_lilst:充当value的列号，列表形式[1,2,4],列号必须递增 
    """
    result = {}
    fin = open(tdfile)
    line_no = 0
    line = fin.readline()
    if len(line.split("\t"))-1 < value_col_list[-1] or len(line.split("\t"))-1 < key_col :
        print "输入的列号大于文件列号"
        sys.exit() 
    while line:
        line_no += 1
        if line_no%500==0:
            print "加载数据 ",line_no," "
        items = line.split("\t")
        if len(items)-1 < value_col_list[-1] or len(items)-1 < key_col:
            print line," 列数小于输入的列数"
        else:
            if result.has_key(items[key_col]):
                pass
            else:
                tmp_list = []
                for i in value_col_list:
                    tmp_list.append(items[i])
                result[items[key_col]] = tmp_list
        line = fin.readline()
    fin.close()
    return result


def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc: # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else: raise

def get_stats_urls(filename):
    """
    从统计局数据查询页面生成的har文件中获取url
    """
    try:
        har = open(filename)
    except Exception,e:
        print e
    else:
        print filename," 文件打开成功"
    
    har_content = har.readlines()
    print len(har_content)
    url_pattern = re.compile('((http|ftp|https)://)(([a-zA-Z0-9\._-]+\.[a-zA-Z]{2,6})|([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}))(:[0-9]{1,4})*(/[a-zA-Z0-9\&%_\./-~-]*)?')
    urls = set()
    for line in har_content:
        m = re.search(url_pattern,line)
        if m:
            urls.add(m.group(0))
    print "供获得:",len(urls),"条url"
    print "条件过滤中....."
    workspace_urls = set()
    for url in urls:
        
        #if url.find(r'workspace/index?') != -1 and url.find(r'tmp=') != -1:
        if url.find(r'm=QueryData&dbcode=hgyd') != -1:
            workspace_urls.add(url)
    print "符合条件的url共",len(workspace_urls),"条"
    urls_file = open("urls.txt","w")
    for url in workspace_urls:
        urls_file.write(url+"\n")

    urls_file.close()
    har.close()
    return workspace_urls

def get_fsnd_data(urls,area_codes_list):
    """
    urls_list:url list
    area_codes_list: 所有地区的编号，用于构造新url
    """
    #需要替换的时间
    time_pattern = re.compile('&time=-1%2C\d{4}&')
    #需要替换的检索地区
    select_pattern = re.compile('&selectId=\d{6}&')
    #日志文件
    log_file = open("url_error.txt","w")
    for area_code in area_codes_list:
        print "当前处理地区:",area_code
        #为地区数据文件创建文件夹
        area_dir = "./data/"+area_code
        if os.path.exists(area_dir):
            pass
        else:
            try:
                os.makedirs(area_dir)
            except  Exception,e:
                print area_dir,"文件夹创建失败",e
                sys.exit()
        #获取数据
        file_no = 0
        for url in urls:
            file_no += 1
            if file_no%50==0:
                print "真个在处理:",file_no,"条数据"
            target_url = url
            target_area = "&selectId="+str(area_code)+"&"
            target_url = re.sub(time_pattern,"&time=-1%2C1949&",target_url)
            target_url = re.sub(select_pattern,target_area,target_url)
            #等在0-3秒，防止被屏蔽
            #获取数据
            wait_time = 3*random.random()
            time.sleep(wait_time)
            try:
                page = urllib.urlopen(target_url)
                data = page.read()
            except Exception,e:
                log_file.write(url+"\n"+e+"\n")
            else:
                pass
            #下载数据
            try:
                file_name = str(file_no)+".dat"
                save_path  = os.path.join(area_dir,file_name)
                fin = open(save_path,"w")
                fin.write(data)
                fin.close()
            except Exception,e:
                print file_no,":",e
            else:
                #print file_no," 文件保存成功"
                pass        
        log_file.close()           
    
    
def data_extract(data_dir):
    print "正在抽取",data_dir,"下的数据"
    filelist = []
    file_no = 0
    current_dir = os.path.dirname(data_dir)
    for root,dirs,files in os.walk(data_dir):
        for file_ in files:
            filelist.append(os.path.join(root,file_))

    indicator_num_filename = os.path.join(current_dir,"tabledata.txt")
    indicator_meta_filename = os.path.join(current_dir,"indicator_meta.txt")
    indicator_num_file = open(indicator_num_filename,"w")
    indicator_meta = open(indicator_meta_filename,"w")
    #region_meta = open("region_meata.txt","w")

    for filename in filelist:
        file_no += 1
        if file_no%500 == 0:
            print "已经处理 ",file_no," 条记录"
        try:
            fin = open(filename)
            #fin = open(tmp_file)
        except Exception,e:
            print e
        else:
            all_data = fin.readlines()
            if len(all_data) == 1:
                try:
                    data_dict = eval(all_data[0])
                except Exception,e:
                    print e
                else:
                    tabledata = data_dict["tableData"]
                    for query,num in  tabledata.iteritems():
                        indi_no,rego_no,year = query.split("_")
                        indicator_num_file.write(indi_no+"\t"+rego_no+"\t"+year+"\t"+num+"\n")
                    
                    value_dict = data_dict["value"]
                    index_list = value_dict["index"]
                    for index_item in index_list:
                        in_id = index_item["id"]
                        in_name = index_item["name"]
                        unit = index_item["unit"]
                        note = index_item["note"]
                        in_ename = index_item["ename"]
                        in_eunit = index_item["eunit"]
                        enote = index_item["enote"]
                        indicator_meta.write(in_id+"\t"+in_name+"\t"+unit+"\t"+note+"\t"+in_ename+"\t"+in_eunit+"\t"+enote+"\n")
                    
                    #region_list = value_dict["region"]
                    #for region_item in region_list:
                    #    rego_id  = region_item["id"]
                    #    rego_name = region_item["name"]
                    #    rego_ename = region_item["ename"]
                    #    region_meta.write(rego_id+"\t"+rego_name+"\t"+rego_ename+"\n")
            fin.close() 


    indicator_num_file.close()
    indicator_meta.close()
    #返回数值文件名和指标元信息文件名
    return [indicator_num_filename,indicator_meta_filename]
    #region_meta.close()

def merge(finame,col,d,foutname):
    """
    
    """
    foutname = os.path.join(os.path.dirname(finame),foutname)
    fin = open(finame)
    fout = open(foutname,"w")
    file_no = 0
    line = fin.readline()
    cols = len(line.split("\t"))
    while line:
        #line = line.strip()
        file_no += 1
        if file_no%1000 == 0:
            print "处理数据: ",file_no,"条"
        items = line.split("\t")
        
        if len(items) == cols:
            tmp_str = ""
            if col == 0:
                pass 
            elif col >=1:
                for i in range(col):
                    tmp_str += items[i]+"\t" 
            else:
                print "输入列参数有错误"
                sys.exit()
            key = items[col]
            if d.has_key(key) :
                tmp_str += key+"\t"
                for d_item in d[key]:
                    tmp_str += d_item.strip() + "\t"
                for i in range(col+1,len(items)-1):
                    tmp_str += items[i].strip() + "\t"
                tmp_str += items[len(items)-1].strip()
                #print items[len(items)-1].strip(),"------"
                #print tmp_str,'____'
                tmp_str += "\n"
                tmp_str2 = tmp_str.strip()
                tmp_str2 = tmp_str2.strip("\t")
                if tmp_str2 != "":
                    fout.write(tmp_str)
                    #print tmp_str,"------"
                else:
                    print items[0]," 字典中没找到，或此条目字典格式不正确" 
            else:
                print  "没有找到与",key,"相对应的值，请修改词典文件"
        else:
            print line,"格式有错误,此行与首行字段个数不一致"    
        line = fin.readline()
        
    fin.close()
    fout.close()
    return foutname

def load_urls(dir_name):
    urls_file_name = os.path.join(dir_name,"urls.txt")
    urls=[]
    try:
        fin = open(urls_file_name)
    except Exception,e:
        print urls_file_name,"文件打开失败",e
        sys.exit()
    else:
        urls = fin.readlines()
        return urls


    
    
def get_data(data_cls,data_dir,area_codes_list):
    """
    根据选择来获取数据
    data_cls:数据分类
    data_dir:数据目录
    """    
    urls = load_urls(data_dir)
    cf = ConfigParser.ConfigParser()
    cf.read("stats_data.conf")
    
    #需要替换的时间
    #time_pattern = re.compile('&time=-1%2C\d{4}&')
    time_pattern = ""
    try:
        time_pattern = cf.get("timepattern", data_cls)
    except Exception,e:
        print "配置文件timepattern获取错误",e
        sys.exit()
        
    btime = ""
    try:
        btime = cf.get("begintime",data_cls)
    except Exception,e:
        print "配置文件begintime参数获取错误",e
        sys.exit()
    #url中获取数据的时间
    btime = "&time=-1%2C" + btime + "&"
        
    time_patt = re.compile(time_pattern)
    #需要替换的检索地区
    select_pattern = re.compile('&selectId=\d{6}&')
    #日志文件
    log_file = open("url_error.txt","w")

    #为地区数据文件创建文件夹
    data_dir = os.path.join(data_dir,"data")
    for area_code in area_codes_list:
        print "当前处理地区:",area_code
        
        #area_dir = "./data/"+area_code
        area_dir = os.path.join(data_dir,area_code)
        if os.path.exists(area_dir):
            pass
        else:
            try:
                os.makedirs(area_dir)
            except  Exception,e:
                print area_dir,"文件夹创建失败",e
                sys.exit()
        #获取数据
        file_no = 0
        for url in urls:
            file_no += 1
            if file_no%50==0:
                print "正在获取第:",file_no,"条数据"
            target_url = url
            target_area = "&selectId="+str(area_code)+"&"
            #target_url = re.sub(time_pattern,"&time=-1%2C1949&",target_url)
            target_url = re.sub(time_patt,btime,target_url)
            target_url = re.sub(select_pattern,target_area,target_url)
            #等在0-3秒，防止被屏蔽
            #获取数据
            wait_time = 3*random.random()
            time.sleep(wait_time)
            try:
                page = urllib.urlopen(target_url)
                data = page.read()
            except Exception,e:
                log_file.write(url+"\n"+e+"\n")
            else:
                pass
            #下载数据
            try:
                file_name = str(file_no)+".dat"
                save_path  = os.path.join(area_dir,file_name)
                fin = open(save_path,"w")
                fin.write(data)
                fin.close()
            except Exception,e:
                print file_no,":",e
            else:
                #print file_no," 文件保存成功"
                pass        
        log_file.close()           
    return data_dir 
    
def letter_quarter(filename,foutname,patterns):
    """
    将包含年份字母的字段转化为年份 季度 ，如2004A  转化为 2004    一季度
    filename 文件名
    col 文件中年份季度的列号，从0开始
    
    """
    foutname = os.path.join(os.path.dirname(filename),foutname)
    fout = open(foutname,"w")
    
    #定义正则表达式替换规则词典
    #patterns = {"(\d{4})A":"\g<1>\t一季度","(\d{4})B":"\g<1>\t二季度","(\d{4})C":"\g<1>\t三季度","(\d{4})D":"\g<1>\t四季度"}

    line_no = 0
    with open(filename) as f:
        for line in f:
            line_no += 1
            if line_no %1000 == 0:
                print "处理数据",line_no,"条"
            items = line.split("\t")
            if len(items) != 7:
                print line,"格式不正确"
            else:
                for k,v in patterns.iteritems():
                    #patt = "'"+k+"'"
                    pattern_str = re.compile(k)
                    #repl_str = "'"+v+"'"
                    if re.search(pattern_str, items[5]):
                        items[5] = re.sub(re.compile(k),v, items[5])
                        for i  in range(len(items)-1):
                            fout.write(items[i]+"\t")
                        fout.write(items[-1])
            
    fout.close()



def stats_data():
    
    print "[1]获取中国宏观年度数据"
    print "[2]获取各省份年度数据"
    print "[3]获取各省份季度数据"
    print "[4]获取各身份月度数据"
    
    data_dir_path = ""
    while True:
        sn = raw_input("输入对应的数字序号进行选择")
        if sn != '1' and sn != '2' and sn !='3' and sn != '4':
            continue
        print "确定",note_menu[sn],"请输入y,否则请输入其他字符.输入0退出程序"
        select = raw_input(":")
        if select == "y":
            current_dir = os.getcwd()
            data_dir_path = os.path.join(current_dir,data_dirs[sn])
            if os.path.exists(data_dir_path):
                pass
            else:
                try:
                    os.makedirs(data_dir_path)
                except  Exception,e:
                    print data_dir_path,"文件夹创建失败",e
                    sys.exit()
            print "相关数据将会保存在:",data_dir_path
            break
        elif select == '0':
            sys.exit()
        else:
            pass
        
    print "载入地区列表中"
    area_code_dict  = load_dict("area_code", 0, [1])
    area_codes = area_code_dict.keys()
    if data_dirs[sn] == "hgnd":
        area_codes = ["000000"]
    else:                                                            #测试
        area_codes =  ['150000', '110000'] #测试
    print "地区列表为：",area_codes
    #获取数据
    #data_dir = get_data(data_dirs[sn],data_dir_path , area_codes)
    #data_dir = '/home/jay/workspace_new/stats_data/main/hgnd/data'
    print "获取数据完成"
    #data_dir = '/home/jay/workspace_new/stats_data/main/fsyd/data'
    #result_names   =  data_extract(data_dir)
    #填充地区名称
    #indi_region_filename = merge(result_names[0],1, area_code_dict,"indicator_regionname.txt")
    #填充指标名称和单位
    #indi_dict =  load_dict(result_names[1],0,[1,2])
    #indi_regin_unit_filename = merge(indi_region_filename,0, indi_dict,"indicator_regionname_unit.txt")
    
    indi_regin_unit_filename = '/home/jay/workspace_new/stats_data/main/fsyd/indicator_regionname_unit.txt'
    if data_dirs[sn] == "fsjd":
        print "拆分时间字段，转化为季度中.."
        patterns = {"(\d{4})A":"\g<1>\t一季度","(\d{4})B":"\g<1>\t二季度","(\d{4})C":"\g<1>\t三季度","(\d{4})D":"\g<1>\t四季度"}
        letter_quarter(indi_regin_unit_filename, "indi_regin_unit_jidu.txt", patterns)
    elif data_dirs[sn] == "fsyd":
        print "拆分时间字段，转化为月度中.."
        patterns = {"(\d{4})01":"\g<1>\t1月份",
                            "(\d{4})02":"\g<1>\t2月份",
                            "(\d{4})03":"\g<1>\t3月份",
                            "(\d{4})04":"\g<1>\t4月份",
                            "(\d{4})05":"\g<1>\t5月份",
                            "(\d{4})06":"\g<1>\t6月份",
                            "(\d{4})07":"\g<1>\t7月份",
                            "(\d{4})08":"\g<1>\t8月份",
                            "(\d{4})09":"\g<1>\t9月份",
                            "(\d{4})10":"\g<1>\t10月份",
                            "(\d{4})11":"\g<1>\t11月份",
                            "(\d{4})12":"\g<1>\t12月份",}
        letter_quarter(indi_regin_unit_filename, "indi_regin_unit_yuedu.txt", patterns)
    else:
        pass

def save_page(url,fname,save_dir='indicator_menu'):
    '''
    保存网页
    '''
    try:
        page = urllib.urlopen(url)
        data = page.read()
        outfile_name = os.path.join(save_dir,fname)
        fout = open(outfile_name,"w")
        fout.write(data)
        fout.close()
        return data
    except Exception,e:
        print url,fname,e
    

global  indicator_list
indicator_list = []
indi_info_list = []
def get_indicator_menu(class_set):
    '''
    获取国家统计局网站指标目录文件
    '''
    #宏观月度数据基本url
    #base_url = 'http://data.stats.gov.cn/easyquery.htm?cn=A01'
    #宏观月度数据基本url
    base_url = 'http://data.stats.gov.cn/easyquery.htm?cn=B01'
    
    false = False
    true = True
    #宏观月度
    #dbcode='hgyd'
    #宏观季度
    dbcode='hgjd'
    wdcode='zb'
    m='getTree'
    
    for cls in class_set:
        #http://data.stats.gov.cn/easyquery.htm?cn=A01&id=A01&dbcode=hgyd&wdcode=zb&m=getTree
        tmp_url = base_url+"&id="+cls+"&dbcode="+dbcode+"&wdcode="+wdcode+"&m="+m
        data = save_page(tmp_url, cls)
        l = eval(data)
        if len(l) == 0:
            indicator_list.append(cls)
            continue
        sub_class_set = set()
        for d in l:
            if d.has_key('isParent'):
                indi_code = d['id']
                if d['isParent']==True:
                    sub_class_set.add(indi_code)   
                else:
                    #yield indi_code
                    indicator_list.append(indi_code)
                    indi_info_list.append(str(d))
                    #tmp_url = base_url+"&id="+indi_code+"&dbcode="+dbcode+"&wdcode="+wdcode+"&m="+m
                    #save_page(tmp_url, indi_code)
        if len(sub_class_set) > 0:
            get_indicator_menu(sub_class_set)
            
def get_hgyd_data(search_file_name,start_year=2014,end_year=2014,dst_dir="hgyd/data/"):
    '''
    获取宏观月度数据
    '''
    log_file_name = "log.dat"
    log = open(log_file_name,"w+")
    search_zb_code = []
    with open(search_file_name) as f:
        lines = f.readlines()
        for line in lines:
            line = line.strip()
            search_zb_code.append(line)
    #
    #base_url = '''http://data.stats.gov.cn/easyquery.htm?m=QueryData&dbcode=hgyd&rowcode=zb&colcode=sj&wds=[]&dfwds=[{"wdcode":"sj","valuecode":"year"}]&k1=zb_code'''
    #宏观月度
    base_url = '''http://data.stats.gov.cn/easyquery.htm?m=QueryData&dbcode=hgyd&rowcode=zb&colcode=sj&wds=[]&dfwds=[{"wdcode":"sj","valuecode":"year"},{"wdcode":"zb","valuecode":"zb_code"}]'''
    #
    #base_url = '''http://data.stats.gov.cn/easyquery.htm?m=QueryData&dbcode=hgjd&rowcode=zb&colcode=sj&wds=[]&dfwds=[{"wdcode":"zb","valuecode":"zb_code"},{"wdcode":"sj","valuecode":"year"}]'''

    for year in range(start_year,end_year+1)[::-1]:
        #year = 2010
        year_str = str(year)
        print "processing:",year_str
        log.write(year_str+"\n")
        save_dir_name = dst_dir + year_str + "/"
        mkdir_p(save_dir_name)
        zb_num = 0
        for zb_code in search_zb_code:
            log.write(zb_code+"\n")
            zb_num += 1
            if zb_num%50 ==0:
                print "Have download ",zb_num,"pages data"
            #zb_code  = 'A0B01'
            target_url = string.replace(base_url, 'year', year_str)
            target_url = string.replace(target_url, 'zb_code',zb_code)
            fname = zb_code+".dat"
            save_page(target_url, fname, save_dir_name)
    log.close()       
    
    
def extra_hgyd_data(data_dir=""):
    '''
    
    '''
    filelist = []
    for root,dirs,files in os.walk(data_dir+"data/"):
        for file_ in files:
            filelist.append(os.path.join(root,file_))
    
    
    data_out_file_name = os.path.join(data_dir,"extra_data.dat")
    zb_out_file_name = os.path.join(data_dir,"zb_info.dat")
    data_out = open(data_out_file_name,"w")
    zb_out = open(zb_out_file_name,"w")
    #写入指标数值字段名
    data_fields = ['统计局指标编号','指标时间','数值','显示数值','小数点位数']
    data_out.write('\t'.join(data_fields))
    data_out.write("\n")
    #写入指标元信息字段名
    zb_fields = ['统计局指标编号','指标_cname','指标_exp','指标_memo','指标_name','指标_tag','指标单位']
    zb_out.write('\t'.join(zb_fields))
    zb_out.write("\n")
    file_no = 0
    zb_set = set()
    #current_dir = os.path.dirname(data_dir)
    #获取所有文件名
    
    #遍历文件
    for file_name in filelist:
        file_no += 1
        if file_no % 100 ==0:
            print "processing:",file_no," file"
        with open(file_name) as f:
            content = f.read()
            try:
                decodejson = json.loads(content)
            except Exception,e:
                print file_name,e
                continue
            returndata = decodejson['returndata']
            datanodes = returndata['datanodes']
            wdnodes = returndata['wdnodes']
            
            #保存指标代码及数值
            for node in datanodes:
                #print node['code']
                zb_sj_code = node['code']
                #获取指标和时间代码
                codes = zb_sj_code.split("_")
                assert len(codes) == 2
                zb_code = codes[0].strip("zb.")
                sj_code  =  codes[1].strip("sj.")
                data = node['data']
                num = str(data['data'])
                dotcount = str(data['dotcount'])
                hasdata = data['hasdata']
                strdata = data['strdata']
                if hasdata:
                    item_list = [zb_code,sj_code,num,strdata.encode('utf-8'),dotcount]
                    data_out.write("\t".join(item_list))
                    data_out.write("\n")
                    #print num,dotcount,hasdata,strdata
            #保存指标信息 
            for wdnode in wdnodes:
                if wdnode['wdcode'] == 'zb':
                    nodes = wdnode['nodes']
                    for node in nodes:
                        code =  node['code']
                        if code in zb_set:
                            pass
                        else:
                            cname =  node['cname']
                            exp =  node['exp']
                            memo =  node['memo']
                            name =  node['name']
                            tag =  node['tag']
                            unit =  node['unit']
                            zb_list = [code.encode('utf-8'),cname.encode('utf-8'),exp.encode('utf-8'),memo.encode('utf-8'),name.encode('utf-8'),tag.encode('utf-8'),unit.encode('utf-8')]
                            zb_out.write("\t".join(zb_list))
                            zb_out.write("\n")
    data_out.close()
    zb_out.close()
                                    
    
#
if __name__ == "__main__":
    #urls = get_stats_urls(sys.argv[1])
    #urls = get_stats_urls("hgyd.har")
    #area_code_dict  = load_dict("area_code", 0, [1])
    #area_codes = area_code_dict.keys()
    #get_fsnd_data(urls, area_codes)
    #data_dir = "./data/"
    #data_extract(data_dir)
    #indi_dict = load_dict("indicator_meta.txt", 0, [1,2])
    #merge("table_data_merge.txt",3,area_code_dict,"tabledata_indi_region.txt")
    #stats_data()
    #hgyd------- 
    #hgyd_class_set = set(['A01','A02','A03','A04','A05','A06','A07','A08','A09','A0A','A0B'])
    #l = get_indicator_menu(hgyd_class_set)
    #hgjd_class_set = set(['A01','A02','A03','A04','A05'])
    #l = get_indicator_menu(hgjd_class_set)
    #print indicator_list
    #fout = open("search_zb.txt","w")
    #fout.write('\n'.join(indicator_list))
    #fout.close()
    
    #get_hgyd_data("hgyd/hgyd_search_zb.txt")
    extra_hgyd_data("hgjd/")
    print "数据处理完毕!   End!"

