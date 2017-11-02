-- set @today=curdate();
-- set @yesterday=date_sub(curdate(), INTERVAL 1 DAY);
-- set @tomorrow=date_sub(curdate(), INTERVAL -1 DAY);
-- set @start_dt=@yesterday;
-- set @stop_dt=@today;
-- 上海
select 
kai_ting_ri_qi,
an_you,
an_hao,
fa_yuan fa_yuan_ming_cheng,
fa_ting shen_li_fa_ting,
shen_pan_zhang zhu_shen_fa_guan,
cheng_ban_bu_men cheng_ban_ting,
concat('原告：',yuan_gao,'；被告：',bei_gao) dang_shi_ren
from `hshfy`
where add_time >=@start_dt and add_time<@stop_dt
union all
-- 广东
select 
kai_ting_shi_jian kai_ting_ri_qi,
'' an_you,
an_hao,
fa_yuan fa_yuan_ming_cheng,
kai_ting_di_dian shen_li_fa_ting,
zhu_shen_fa_guan,
'' cheng_ban_ting,
dang_shi_ren
from `gdcourts`
where add_time >=@start_dt and add_time<@stop_dt
union all
-- 浙江
select 
kai_ting_ri_qi,
an_you,
an_hao,
fa_yuan fa_yuan_ming_cheng,
fa_ting shen_li_fa_ting,
shen_pan_zhang zhu_shen_fa_guan,
cheng_ban_bu_men cheng_ban_ting,
concat(yuan_gao,'；',bei_gao) dang_shi_ren
from `zjsfgkw`
where add_time >=@start_dt and add_time<@stop_dt
union all
-- 江苏-南京
select 
ri_qi kai_ting_ri_qi,
an_you,
an_hao,
'南京市中级人民法院' fa_yuan_ming_cheng,
fa_ting shen_li_fa_ting,
zhu_shen zhu_shen_fa_guan,
'' cheng_ban_ting,
dang_shi_ren
from `js_nanjing`
where updatetime>=@start_dt and updatetime<@stop_dt
union all
-- 江苏-扬州
select
shi_jian kai_ting_ri_qi,
an_you,
an_hao,
'扬州市中级人民法院' fa_yuan_ming_cheng,
fa_ting shen_li_fa_ting,
zhu_shen zhu_shen_fa_guan,
'' cheng_ban_ting,
concat('原告：',yuan_gao,'；被告',bei_gao) dang_shi_ren
from `js_yangzhou`
where updatetime>=@start_dt and updatetime<@stop_dt
union all
-- 江苏-镇江
select 
replace(ri_qi,'日　期：','') kai_ting_ri_qi,
replace(an_you,'案　由：','') an_you,
replace(an_hao,'案　号：','') an_hao,
'江苏省镇江市中级人民法院' fa_yuan_ming_cheng,
replace(fa_ting,'地　点：','') shen_li_fa_ting,
'' zhu_shen_fa_guan,
'' cheng_ban_ting,
dang_shi_ren
from `js_zhenjiang`
where updatetime>=@start_dt and updatetime<@stop_dt
union all
-- 江苏-苏州
select 
ri_qi kai_ting_ri_qi,
an_you,
an_hao,
'苏州市中级人民法院' fa_yuan_ming_cheng,
di_dian shen_li_fa_ting,
shen_pan_zhang zhu_shen_fa_guan,
'' cheng_ban_ting,
dang_shi_ren
from `szfy`
where add_time >=@start_dt and add_time<@stop_dt
-- 山东中级法院
union all
select 
kai_ting_ri_qi,
an_you,
'' an_hao,
fa_yuan fa_yuan_ming_cheng,
fa_ting shen_li_fa_ting,
shen_pan_zhang zhu_shen_fa_guan,
'' cheng_ban_ting,
concat('原告：',yuan_gao,'；被告：',bei_gao) dang_shi_ren
from `sdzjfy`
where add_time >=@start_dt and add_time<@stop_dt
-- 江苏-无锡（字段对应有误当事人字段待解析）
-- 江苏-泰州（当事人字段待解析）
-- 江苏常州（当事人、案由字段待解析）
-- 江苏徐州（字段待解析）
-- 江苏南通（当事人、案由字段待解析）
-- 江苏连云港（当事人、案由字段待解析）
-- 山东高院（字段待解析）
-- 福建厦门(当事人字段待解析)
-- 福建高院（当事人案由字段待解析）
-- 北京（字段待解析）