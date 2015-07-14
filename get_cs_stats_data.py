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

global  indicator_list
indicator_list = []
indi_info_list = []

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

def load_list(fname):
    '''
    载入列表,每行为列表的一个元素
    '''
    result = []
    with open(fname) as f:
        lines = f.readlines()
        for line in lines:
            result.append(line.strip())
    return result
    

def del_tabs(s):
    '''
    
    '''
    result_str = s.strip()
    result_str = string.replace(result_str, "\t", " ")
    result_str = string.replace(result_str, "\n", " ")
    return result_str
    
def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc: # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else: raise

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

def save_page(url,fname,save_dir):
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
    
def get_zb_tree(class_set,dbcode,base_url,data_dir):
    '''
    获取国家统计局网站指标树
    '''
    #宏观月度数据基本url
    #base_url = 'http://data.stats.gov.cn/easyquery.htm?cn=A01'
    #宏观季度数据基本url
    #base_url = 'http://data.stats.gov.cn/easyquery.htm?cn=B01'
    false = False
    true = True
    #宏观月度
    #dbcode='hgyd'
    #宏观季度
    #dbcode='hgjd'
    wdcode='zb'
    m='getTree'
    
    for cls in class_set:
        #http://data.stats.gov.cn/easyquery.htm?cn=A01&id=A01&dbcode=hgyd&wdcode=zb&m=getTree
        tmp_url = base_url+"&id="+cls+"&dbcode="+dbcode+"&wdcode="+wdcode+"&m="+m
        data = save_page(tmp_url, cls,data_dir)
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
                    indicator_list.append(indi_code)
                    indi_info_list.append(str(d))
        if len(sub_class_set) > 0:
            get_zb_tree(sub_class_set, dbcode, base_url,data_dir)
            
def get_cls_data(search_zb_code,base_url,dst_dir="hgyd/data/",area_code_list=None,start_year=2000,end_year=2014):
    '''
    获取宏观分类数据
    '''
    log_file_name = "log.dat"
    log = open(log_file_name,"a")
    #
    #base_url = '''http://data.stats.gov.cn/easyquery.htm?m=QueryData&dbcode=hgyd&rowcode=zb&colcode=sj&wds=[]&dfwds=[{"wdcode":"sj","valuecode":"year"}]&k1=zb_code'''
    #
    #base_url = '''http://data.stats.gov.cn/easyquery.htm?m=QueryData&dbcode=hgyd&rowcode=zb&colcode=sj&wds=[]&dfwds=[{"wdcode":"sj","valuecode":"year"},{"wdcode":"zb","valuecode":"zb_code"}]'''
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
            #判断是否有地区列表
            if  area_code_list is None:
                pass
            else:
                for area_code in area_code_list:
                    area_save_dir_name = save_dir_name + area_code + "/"
                    area_target_url = string.replace(target_url, 'cs_code',area_code)
                    #save_dir_name = save_dir_name + area_code + "/"
                    if not  os.path.exists(area_save_dir_name):
                        mkdir_p(area_save_dir_name)
                    fname = zb_code+".dat"
                    save_page(area_target_url, fname, area_save_dir_name)
                continue
            fname = zb_code+".dat"
            save_page(target_url, fname, save_dir_name)
    log.close()       
    print "download done!"
    
def extra_hg_data(data_cls):
    '''
    
    '''
    area_cls = ['csyd','csnd']
    print "抽取",data_cls,"数据"
    search_dir = data_cls + "/data/"
    filelist = []
    if os.path.exists(search_dir):
        for root,dirs,files in os.walk(search_dir):
            for file_ in files:
                filelist.append(os.path.join(root,file_))
    else:
        print "don't find ",search_dir
        sys.exit()
    data_out_file_name = os.path.join(data_cls,"extra_data.dat")
    zb_out_file_name = os.path.join(data_cls,"zb_info.dat")
    if data_cls in area_cls:
        dq_out_file_name = os.path.join(data_cls,"dq_info.data")
        dq_out = open(dq_out_file_name,"w")
    data_out = open(data_out_file_name,"w")
    zb_out = open(zb_out_file_name,"w")
    #写入指标数值字段名
    data_fields = ['统计局指标编号','地区代码','指标时间','数值','显示数值','小数点位数']
    data_out.write('\t'.join(data_fields))
    data_out.write("\n")
    #写入指标元信息字段名
    zb_fields = ['统计局指标编号','指标_cname','指标_exp','指标_memo','指标_name','指标_tag','指标单位']
    zb_out.write('\t'.join(zb_fields))
    zb_out.write("\n")
    #写入地区元信息字段名
    if data_cls in area_cls:
        dq_fields = ['地区代码','地区名称']
        dq_out.write('\t'.join(dq_fields))
        dq_out.write("\n")
    
    file_no = 0
    zb_set = set()
    dq_set = set()
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
                if data_cls in area_cls:
                    assert len(codes) == 3
                    zb_code = codes[0].strip("zb.")
                    dq_code  =  codes[1].strip("reg.")
                    sj_code  =  codes[2].strip("sj.")
                else:
                    assert len(codes) == 2
                    zb_code = codes[0].strip("zb.")
                    dq_code = "000000"
                    sj_code  =  codes[1].strip("sj.")
                data = node['data']
                num = str(data['data'])
                dotcount = str(data['dotcount'])
                hasdata = data['hasdata']
                strdata = data['strdata']
                if hasdata:
                    item_list = [zb_code,dq_code,sj_code,num,strdata.encode('utf-8'),dotcount]
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
                            zb_set.add(code)
                            cname =  node['cname']
                            cname = del_tabs(cname)
                            exp =  node['exp']
                            exp = del_tabs(exp)
                            memo =  node['memo']
                            memo = del_tabs(memo)
                            name =  node['name']
                            name = del_tabs(name)
                            tag =  node['tag']
                            tag = del_tabs(tag)
                            unit =  node['unit']
                            unit = del_tabs(unit)
                            zb_list = [code.encode('utf-8'),cname.encode('utf-8'),exp.encode('utf-8'),memo.encode('utf-8'),name.encode('utf-8'),tag.encode('utf-8'),unit.encode('utf-8')]
                            zb_out.write("\t".join(zb_list))
                            zb_out.write("\n")
                if wdnode['wdcode'] == 'reg' and data_cls in area_cls:
                    nodes = wdnode['nodes']
                    for node in nodes:
                        code =  node['code']
                        if code in dq_set:
                            pass
                        else:
                            cname =  node['cname']
                            cname = del_tabs(cname)
                            dq_list = [code.encode('utf-8'),cname.encode('utf-8')]
                            dq_out.write("\t".join(dq_list))
                            dq_out.write("\n")
                            dq_set.add(code)
    data_out.close()
    zb_out.close()
    if data_cls in area_cls:
        dq_out.close()
    
def table_to_rec(table_file_name):
    '''
    
    '''
    sufix = os.path.splitext(table_file_name)[1]
    pos = table_file_name.find(sufix)
    rec_file_name = table_file_name[:pos]+"_REC"+sufix
    fout = open(rec_file_name,"w")
    
    fin = open(table_file_name)
    line = fin.readline()
    fields_str = line.strip()
    fields = fields_str.split("\t")
    fields_num = len(fields)
    line_no = 0
    while line:
        line_no += 1
        if line_no % 1000 == 0:
            print "processing:",line_no,"lines"
        rec_items = []
        line = fin.readline()
        #record = line.strip()
        items = line.split("\t")
        if len(items) == fields_num:
            rec_items.append("<REC>")
            for i  in range(fields_num):
                rec_items.append("<"+fields[i]+">="+items[i])
            fout.write("\n".join(rec_items))
            #fout.write("\n")
        else:
            print line,"length:",len(items),"切分后个数不等于字段个数"
            for tmp in items:
                print tmp
        line = fin.readline()
    fout.close()
                
def download_hg_stats_data():
    '''
    下载宏观统计数据
    '''
    note_menu = {'1':'获取宏观月度数据', 
                                '2':'获取宏观季度数据',
                                '3':'获取宏观年度数据',
                                '4':'获取城市月度数据',
                                '5':'获取城市年度数据',
                                '6':'获取分省月度数据',
                                '7':'获取分省季度数据',
                                '8':'获取分省年度数据'}
    
    data_dirs     = {'1':'hgyd', 
                                '2':'hgjd',
                                '3':'hgnd',
                                '4':'csyd',
                                '5':'csnd',
                                '6':'fsyd',
                                '7':'fsjd',
                                '8':'fsnd',}
    
    print "[1]获取宏观月度数据"
    print "[2]获取宏观季度数据"
    print "[3]获取宏观年度数据"
    print "[4]获取城市月度数据"
    print "[5]获取城市年度数据"
    print "[6]获取分省月度数据"
    print "[7]获取分省季度数据"
    print "[8]获取分省年度数据"
    
    #indicator_list = []
    db_cls = ""
    area_list = None
    #创建对应的文件夹
    while True:
        sn = raw_input("输入对应的数字序号进行选择")
        if sn != '1' and sn != '2' and sn !='3' and sn !='4' and sn !='5':
            continue
        print "确定",note_menu[sn],"请输入y,否则请输入其他字符.输入0退出程序"
        select = raw_input(":")
        if select == "y":
            current_dir = os.getcwd()
            data_dir_path = os.path.join(current_dir,data_dirs[sn])
            if not os.path.exists(data_dir_path):
                try:
                    mkdir_p(data_dir_path)
                except  Exception,e:
                        print data_dir_path,"文件夹创建失败",e
                        sys.exit()
            print note_menu[sn]," 数据将会保存在: ",data_dir_path
            db_cls = data_dirs[sn]
            break
        elif select == '0':
            sys.exit()
        else:
            pass
    #读取配置文件
    #得到 获取指标树所需要的url
    cf = ConfigParser.ConfigParser()
    cf.read("stats_data.conf")
    #get_tree_url = ""
    try:
        get_tree_url = cf.get("gettreeurl", db_cls)
    except Exception,e:
        print "配置文件中get_tree_url加载错误",e
        sys.exit()
    #加载当前数据类型所对应的顶层指标分类编号列表,转化为集合
    #zb_cls = set()
    try:
        zb_cls = set(eval(cf.get("zbcls", db_cls)))
    except Exception,e:
        print "配置文件中zb_cls加载错误",e
        sys.exit()
    #加载指标数据检索字url
    try:
        queryurl = cf.get("queryurl", db_cls)
    except Exception,e:
        print "配置文件中queryurl加载错误",e
        sys.exit()
    #从统计局获取所有指标的父类目
    zb_tree_dir = os.path.join(data_dir_path,"zb_tree/")
    if not os.path.exists(zb_tree_dir):
        mkdir_p(zb_tree_dir)
    #判断检索指标类别文件是否存在,不存在则下载
    search_cls = []
    search_zb_file_name = os.path.join(data_dir_path,"search_cls.txt")
    if os.path.exists(search_zb_file_name):
        with open(search_zb_file_name) as f:
            lines = f.readlines()
            for line in lines:
                search_cls.append(line.strip())
    else:
        get_zb_tree(zb_cls, db_cls, get_tree_url,zb_tree_dir)
        search_cls = indicator_list
        with open(search_zb_file_name,"w") as f:
            f.write('\n'.join(search_cls))
            f.write('\n')
    #逐年、逐指标类目下载数据
    #创建数据存放目录
    download_dir = os.path.join(data_dir_path,"data/")
    if os.path.exists(download_dir):
        mkdir_p(download_dir)
    #是否获取地区代码
    if sn == '4' or sn == '5':
        area_list = load_list("dq_code/"+data_dirs[sn]+".dat")
    get_cls_data(search_cls, queryurl,download_dir,area_list)
    #extra_hg_data(download_dir)
    print note_menu[sn],"数据获取完成"
    

#
if __name__ == "__main__":
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
    #extra_hgyd_data("hgyd/")
    #table_to_rec("hgyd/extra_data.dat")
    #table_to_rec("hgjd/zb_info.dat")
    
    #indi_dict = load_dict("hgjd/zb_info.dat", 0, [1,6])
    #merge("hgjd/extra_data.dat", 0, indi_dict, "extra_zbname_unit_data.dat")
    #table_to_rec("hgjd/extra_zbname_unit_data.dat")
    #download_hg_stats_data()
    extra_hg_data("csyd")
    print "数据处理完毕!   End!"

