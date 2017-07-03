import sys
import random
from random import randint


def genQuery(input):
    line =input #str(input).split("~")
    #print (line)
    dq_check_id = line[0]
    dq_check_nm = line[1]
    dq_group_id = line[2]
    dq_app_id = line[3]
    src_schema = line[4]
    src_tbl = line[5]
    src_col = line[6]
    rowkey = line[7]
    filter = line[8]
    dq_thrshold_per = line[9]
    #print(dq_check_nm,rowkey)
    nullChkFlag = line[10]
    dupChkFlag = line[11]
    lenChkMaxMin = line[12]
    lovList = line[13]
    data_type_chk = line[14]
    format_chk = line[15]
    ref_chk_schema = line[16]
    ref_chk_tbl = line[17]
    ref_chk_col = line[18]
    ref_chk_filter = line[19]
    custom_chk = line[20]
    error_rec_limit = line[21]
    active_flag = line[22]
    last_updt_dt = line[23]
    #print ("Ref Flag",chkFlag(ref_chk_col))
    nullChkRtn = nullChk(src_tbl, src_col, chkFlag(nullChkFlag))
    lovChkRtn = lovChk(src_tbl, src_col,lovList, chkFlag(lovList))
    lenChkRtn = lenChk(src_tbl, src_col, lenChkMaxMin,chkFlag(lenChkMaxMin))
    refChkRtn = refChk(src_tbl, src_col,ref_chk_schema,ref_chk_tbl,ref_chk_col,chkFlag(ref_chk_col))
    # Need to be developed
    dupChkRtn = dupChk()
    print ("dupChkRtn",dupChkRtn)
    dataTypeChkRtn = dataTypeChk()
    formatChkRtn = formatChk()
    customChkRtn = customChk()
    #print ("Ref Flag len ",refChkRtn[0:2].strip(),len(refChkRtn[0:2].strip()))
    reqVar = dq_check_id+"~"+src_schema+"~"+src_tbl+"~"+src_col+"~"+dq_check_nm+"~"+dq_group_id+"~"+\
    dq_app_id+"~"+rowkey+"~"+dq_thrshold_per

    return reqVar,dtlQuery(src_tbl,rowkey,src_col, nullChkRtn,dupChkRtn, lenChkRtn, lovChkRtn,dataTypeChkRtn,formatChkRtn,\
    customChkRtn,refChkRtn)

def chkFlag(flag):
    return [flag if len(flag) != 0 else 'N'][0]

def rowKeyValCal(tbl,rowkey):
    rowKeyList = rowkey.split(",")
    rowKeyOut  = 'concat_ws("~"'
    for i in range(0,len(rowKeyList)):
        #print (i,rowKeyList[i])
        #cast(cust_prod_detl.sales_id as string)
        rowKeyOut =  rowKeyOut + ' , cast('+tbl+'.'+rowKeyList[i] + ' as string )'
    #return [flag if len(flag) != 0 else 'N'][0]
    return rowKeyOut + ') as rowKeyVal '



def dtlQuery(tbl,rowkey,col,nullChkRtn,dupChkRtn, lenChkRtn, lovChkRtn,dataTypeChkRtn,formatChkRtn,\
    customChkRtn,refChkRtn):
    fltrStart = ' select * from ('
    fltrEnd = ' ) innerQuery where (nullChk = 1 or dupChk = 1 or lenChk = 1 or lovChk = 1 or dataTypeChk = 1 or formatChk = 1 or customChk = 1 or refChk = 1 )'
    if (refChkRtn[0:4]) != 'CASE' :
        s = 'SELECT '+ rowKeyValCal(tbl,rowkey)+','+tbl+'.'+col+',' + nullChkRtn+','+dupChkRtn+','+lenChkRtn+','+lovChkRtn+ ','+dataTypeChkRtn +','+formatChkRtn+','+customChkRtn+','+refChkRtn+' FROM ' +tbl #+';'
    else :
        s = 'SELECT '+ rowKeyValCal(tbl,rowkey)+','+ tbl+'.'+col+',' + nullChkRtn+','+dupChkRtn+','+lenChkRtn+','+lovChkRtn+ ','+dataTypeChkRtn +','+formatChkRtn+','+customChkRtn+','+refChkRtn #+ ';'

    return fltrStart+s+fltrEnd

def nullChk(tbl,col,chk='Y'):
    if chk == 'N' :
        return '"" AS nullChk'
    else:
        s= 'CASE WHEN ('+tbl+'.'+col + ' IS NULL OR ' + tbl+'.'+col +' = "") THEN 1 ELSE 0 END AS nullChk'
    return s

def lovChk(tbl,col,valList,chk='Y'):
    if chk == 'N':
        return '"" AS lovChk'
    else :
        s= 'CASE WHEN ('+tbl+'.'+col + ' NOT IN ' + str(valList)+ ') THEN 1 ELSE 0 END  AS lovChk'
    return s

def lenChk(tbl,col,max_min,chk='Y'):
    if chk == 'N':
        return '"" AS lenChk'
    else:
        s= 'CASE WHEN (length(' + tbl+'.'+col + ') > ' + str(max_min[3]) + ' or  length(' + tbl+'.'+col + ') < ' + str(max_min[1]) +') THEN 1 ELSE 0 END  AS lenChk'
    return s

def refChk(tbl,col,tgt_schema,tgt_tbl,tgt_col,chk='Y'):
    if chk == 'N':
        return '"" AS refChk'
    else :
        s= 'CASE WHEN ('+tgt_tbl+'.'+tgt_col + ' IS NULL  ) THEN 1 ELSE 0 END  AS refChk FROM ' + tbl +' left outer join '+tgt_schema + \
        '.'+tgt_tbl+ ' on ( ' + tbl+'.'+col+ ' = ' + tgt_tbl +'.'+tgt_col + ')'
    return s

def dupChk():
    return '"" AS dupChk'

def dataTypeChk():
    return '"" AS dataTypeChk'

def formatChk():
    return '"" AS formatChk'

def customChk():
    return '"" AS customChk'

    


#print (genQuery(input))
