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
    formatChkRtn = formatChk(src_tbl, src_col,format_chk,chkFlag(format_chk))
    dupChkRtn = dupChk(src_tbl,rowkey,chkFlag(dupChkFlag))
    dataTypeChkRtn = dataTypeChk()
    customChkRtn = customChk()
    #refChkRtn = refChk(src_tbl, src_col,ref_chk_schema,ref_chk_tbl,ref_chk_col)
    # Need to be developed
    print (dupChk(src_tbl,rowkey))
    print ("format_chk",format_chk)
    #print ("Ref Flag len ",refChkRtn[0:2].strip(),len(refChkRtn[0:2].strip()))
    reqVar = dq_check_id+"~"+src_schema+"~"+src_tbl+"~"+src_col+"~"+dq_check_nm+"~"+dq_group_id+"~"+\
    dq_app_id+"~"+rowkey+"~"+dq_thrshold_per

    return reqVar,dtlQuery(src_tbl,rowkey,src_col, nullChkRtn,dupChkRtn, lenChkRtn, lovChkRtn,dataTypeChkRtn,formatChkRtn,\
    customChkRtn,refChkRtn)

def chkFlag(flag):
    return ['Y' if len(flag) != 0 else 'N'][0]

def rowKeyValCal(tbl,rowkey):
    rowKeyList = rowkey.split(",")
    rowKeyOut  = 'concat_ws("~"'
    for i in range(0,len(rowKeyList)):
        #print (i,rowKeyList[i])
        #cast(cust_prod_detl.sales_id as string)
        rowKeyOut =  rowKeyOut + ' , cast('+tbl+'.'+rowKeyList[i] + ' as string )'
    #return [flag if len(flag) != 0 else 'N'][0]
    return rowKeyOut + ')'

'''select *, CASE WHEN (duprowKeyVal IS NOT NULL) THEN 1 ELSE 0 END dupChk from (SELECT concat_ws("~" , cast(cust_prod_detl.sales_id  as string ) , cast(cust_prod_detl.load_date as string )) as rowKeyVal ,cust_prod_detl.country_cd,CASE WHEN (cust_prod_detl.country_cd IS NULL OR cust_prod_detl.country_cd = "") THEN 1 ELSE 0 END AS nullChk,"" AS dupChk,CASE WHEN (length(cust_prod_detl.country_cd) > 3 or  length(cust_prod_detl.country_cd) < 2) THEN 1 ELSE 0 END  AS lenChk,CASE WHEN (cust_prod_detl.country_cd NOT IN ('USA','IND')) THEN 1 ELSE 0 END  AS lovChk,"" AS dataTypeChk,CASE WHEN (cust_prod_detl.country_cd RLIKE '[^0-9]' ) THEN 0 ELSE 1 END AS formatChk,"" AS customChk,CASE WHEN (country_state_dtl.country_cd IS NULL  ) THEN 1 ELSE 0 END  AS refChk FROM cust_rf.cust_prod_detl left outer join cust_rf.country_state_dtl on ( cust_prod_detl.country_cd = country_state_dtl.country_cd)  ) innerQuery1 left outer join ( select concat_ws("~" , cast(cust_prod_detl.sales_id  as string ) , cast(cust_prod_detl.load_date as string )) as duprowKeyVal from cust_rf.cust_prod_detl group by concat_ws("~" , cast(cust_prod_detl.sales_id  as string ) , cast(cust_prod_detl.load_date as string )) having count(1) >1  ) dupTbl on rowKeyVal = duprowKeyVal '''
    
def dtlQuery(tbl,rowkey,col,nullChkRtn,dupChkRtn, lenChkRtn, lovChkRtn,dataTypeChkRtn,formatChkRtn,\
    customChkRtn,refChkRtn):
    fltrStart = ' select * from ('
    fltrEnd = ' ) innerQuery where (nullChk = 1 or dupChk = 1 or lenChk = 1 or lovChk = 1 or dataTypeChk = 1 or formatChk = 1 or customChk = 1 or refChk = 1 )'
    dupFiltrStart = ' select innerQuery1.*, CASE WHEN (duprowKeyVal IS NOT NULL) THEN 1 ELSE 0 END AS dupChk from ('
    dupFiltrEnd1 = ' )innerQuery1 left outer join ('
    dupFiltrEnd2 = ' ) dupTbl on rowKeyVal = duprowKeyVal '

    if (refChkRtn[0:4]) != 'CASE' and dupChkRtn[0:6] !='SELECT' :
        s = 'SELECT '+ rowKeyValCal(tbl,rowkey)+ ' as rowKeyVal '+ ','+tbl+'.'+col+',' + nullChkRtn+','+dupChkRtn+','+lenChkRtn+','+lovChkRtn+ ','+dataTypeChkRtn +','+\
        formatChkRtn+','+customChkRtn+','+refChkRtn+' FROM ' +tbl #+';'
        return fltrStart+s+fltrEnd
    elif (dupChkRtn[0:6] !='SELECT' and refChkRtn[0:4] == 'CASE') :
        s = 'SELECT '+ rowKeyValCal(tbl,rowkey)+ ' as rowKeyVal '+ ','+tbl+'.'+col+',' + nullChkRtn+','+dupChkRtn+','+lenChkRtn+','+lovChkRtn+ ','+dataTypeChkRtn +','+\
        formatChkRtn+','+customChkRtn+','+refChkRtn #+ ';'
        return fltrStart+s+fltrEnd
    elif (dupChkRtn[0:6] =='SELECT' and refChkRtn[0:4] != 'CASE') :
        s = 'SELECT '+ rowKeyValCal(tbl,rowkey)+ ' as rowKeyVal '+ ','+tbl+'.'+col+',' + nullChkRtn+','+lenChkRtn+','+lovChkRtn+ ','+dataTypeChkRtn +','+\
        formatChkRtn+','+customChkRtn+','+refChkRtn+' FROM ' +tbl #+';
        return dupFiltrStart +s +dupFiltrEnd1+dupChkRtn +dupFiltrEnd2
    else :
        s = 'SELECT '+ rowKeyValCal(tbl,rowkey)+ ' as rowKeyVal '+ ','+tbl+'.'+col+',' + nullChkRtn+','+lenChkRtn+','+lovChkRtn+ ','+dataTypeChkRtn +','+\
        formatChkRtn+','+customChkRtn+','+refChkRtn #+ ';'
        return dupFiltrStart +s +dupFiltrEnd1+dupChkRtn +dupFiltrEnd2
        

    

def nullChk(tbl,col,chk='N'):
    if chk == 'N' :
        return '"" AS nullChk'
    else:
        s= 'CASE WHEN ('+tbl+'.'+col + ' IS NULL OR ' + tbl+'.'+col +' = "") THEN 1 ELSE 0 END AS nullChk'
    return s

def lovChk(tbl,col,valList,chk='N'):
    if chk == 'N':
        return '"" AS lovChk'
    else :
        s= 'CASE WHEN ('+tbl+'.'+col + ' NOT IN ' + str(valList)+ ') THEN 1 ELSE 0 END  AS lovChk'
    return s

def lenChk(tbl,col,max_min,chk='N'):
    if chk == 'N':
        return '"" AS lenChk'
    else:
        s= 'CASE WHEN (length(' + tbl+'.'+col + ') > ' + str(max_min[3]) + ' or  length(' + tbl+'.'+col + ') < ' + str(max_min[1]) +') THEN 1 ELSE 0 END  AS lenChk'
    return s

def refChk(tbl,col,tgt_schema,tgt_tbl,tgt_col,chk='N'):
    if chk == 'N':
        return '"" AS refChk'
    else :
        s= 'CASE WHEN ('+tgt_tbl+'.'+tgt_col + ' IS NULL  ) THEN 1 ELSE 0 END  AS refChk FROM ' + tbl +' left outer join '+tgt_schema + \
        '.'+tgt_tbl+ ' on ( ' + tbl+'.'+col+ ' = ' + tgt_tbl +'.'+tgt_col + ')'
    return s

def dupChk(tbl,rowkey,chk='N'):
    if chk =='N':
        return '"" AS dupChk'
    else :
        s = 'SELECT '+ rowKeyValCal(tbl,rowkey)+' as dupRowKeyVal  FROM ' +tbl + ' group by ' + rowKeyValCal(tbl,rowkey) + ' having count(1) >1'
    return s

def dataTypeChk():
    return '"" AS dataTypeChk'

def formatChk(tbl,col,fmtType,chk='N'):
    if chk == 'N':
        return '"" AS formatChk'
    else :
        return 'CASE WHEN ('+tbl+'.'+col + ' RLIKE '+ fmtType + ' ) THEN 0 ELSE 1 END AS formatChk'

def customChk():
    return '"" AS customChk'

    


#print (genQuery(input))
