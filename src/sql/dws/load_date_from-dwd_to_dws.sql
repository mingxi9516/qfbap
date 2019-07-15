insert into qfbap_dws.dws_user_visit_month1 partition(dt="20190708")
select
t.user_id,
t.type,
t.cnt,
t.connect,
row_number() over(distribute by t.user_id,t.type sort by t.cnt desc) rn,
t.dw_date
from
(
select
user_id,
'visit_ip' as type,
sum(pv) as cnt,
visit_ip as connect,
current_timestamp dw_date
from
qfbap_dwd.dwd_user_pc_pv
group by user_id,visit_ip
union all
select
user_id,
'cookie_id' as type,
sum(pv) as cnt,
cookie_id as connect,
current_timestamp dw_date
from
qfbap_dwd.dwd_user_pc_pv
group by user_id,cookie_id
union all
select
user_id,
'session_id' as type,
sum(pv) as cnt,
session_id as connect,
current_timestamp dw_date
from
qfbap_dwd.dwd_user_pc_pv
group by user_id,session_id
union all
select
user_id,
'visit_os' as type,
sum(pv) as cnt,
visit_os as connect,
current_timestamp dw_date
from
qfbap_dwd.dwd_user_pc_pv
group by user_id,visit_os
) t;
------------------------------------------------------------------------------------
select
user_id,
count(distinct(case when type="visit_ip" then content end)) dt30_dis_ip_count,--近30天访问使用的不同ip数量
max(case when rn=1 and type="visit_ip" then content end) dt30_max_ip,--近30天最常用的ip
count(case when type="cookie_id" then content end) dt30_sum_cookie_ip,--近30天使用的cookie的数量
max(case when rn=1 and type="cookie_id" then content end) dt30_max_cookie_ip,--近30使用最常用的cookie_id
max(case when rn=1 and type="visit_os" then content end) dt30_max_visit_os--近30天使用最常用系统
from
qfbap_dws.dws_user_visit_month1
group by user_id;
------------------------------------------------------------------------------------------
select
t2.user_id,
t2.recently_time,
t2.recently_session,
t2.recently_cookie,
t2.recently_pv,
t2.recently_browser_name,
t2.recently_visit_os,
t2.first_time,
t2.first_session,
t2.first_cookie,
t2.first_pv,
t2.first_browser_name,
t2.first_visit_os,
t2.dt7_count,
t2.dt15_count,
t2.dt30_count,
t2.dt60_count,
t2.dt90_count,
t2.dt30_pc_day,
t2.dt30_pv_count,
t2.dt30_avg_pv,
t2.dt30_0t5_pv,
t2.dt30_6t7_pv,
t2.dt30_8t9_pv,
t2.dt30_10t11_pv,
t2.dt30_12t13_pv,
t2.dt30_14t16_pv,
t2.dt30_17t19_pv,
t2.dt30_18t19_pv,
t2.dt30_20t21_pv,
t2.dt30_22t23_pv,
t3.dt30_dis_ip_count,
t3.dt30_max_ip,
t3.dt30_sum_cookie_ip,
t3.dt30_max_cookie_ip,
t3.dt30_max_visit_os
from
(
select
t1.user_id,
max(case when t1.rn_desc=1 then t1.in_time end) recently_time, --最近一次访问时间
max(case when t1.rn_desc=1 then t1.session_id end) recently_session, --最近一次访问使用的session
max(case when t1.rn_desc=1 then t1.cookie_id end) recently_cookie,--最近一次使用的coookie
max(case when t1.rn_desc=1 then t1.pv end) recently_pv,--最近一次的pc端的pv量
max(case when t1.rn_desc=1 then t1.browser_name end) recently_browser_name,--最近一次访问使用的浏览器
max(case when t1.rn_desc=1 then t1.visit_os end) recently_visit_os,--最近一次访问使用的操作系统
max(case when t1.rn_asc=1 then t1.in_time end) first_time,--第一次pc端访问的日期
max(case when t1.rn_asc=1 then t1.session_id end) first_session,--第一次pc端访问的session
max(case when t1.rn_asc=1 then t1.cookie_id end) first_cookie,--第一次pc端访问的cookie
max(case when t1.rn_asc=1 then t1.pv end) first_pv,--第一次访问的pv
max(case when t1.rn_asc=1 then t1.browser_name end) first_browser_name,--第一次访问使用的浏览器
max(case when t1.rn_asc=1 then t1.visit_os end) first_visit_os,--第一次访问的os
sum(t1.dt7) dt7_count,--PC连续7天访问次数
sum(t1.dt15) dt15_count,--连续15天访问次数
sum(t1.dt30) dt30_count,--连续30天访问次数
sum(t1.dt60) dt60_count,--连续60天访问的次数
sum(t1.dt90) dt90_count,--	连续90天访问的次数
count(distinct(case when t1.dt30=1 then substr(t1.in_time,0,10) end )) dt30_pc_day,--近30天pc端访问的次数
sum(case when t1.dt30=1 then t1.pv else 0 end) dt30_pv_count,--近30天pc端的pv
sum(case when t1.dt30=1 then t1.pv else 0 end)/count(distinct(case when t1.dt30=1 then substr(t1.in_time,0,10) end )) dt30_avg_pv,--近30天pc端每天的平均pv
sum(case when t1.dt30=1 and t1.0t5=1 then t1.pv end) dt30_0t5_pv,--	近30天的0到5点的pv数量
sum(case when t1.dt30=1 and t1.6t7=1 then t1.pv end) dt30_6t7_pv,--近30天的6到7点的pv数量
sum(case when t1.dt30=1 and t1.8t9=1 then t1.pv end) dt30_8t9_pv,--近30天的8到9的pv数量
sum(case when t1.dt30=1 and t1.10t11=1 then t1.pv end) dt30_10t11_pv,--近30天的10到11的pv数量
sum(case when t1.dt30=1 and t1.12t13=1 then t1.pv end) dt30_12t13_pv,--近30天的12到13的pv数量
sum(case when t1.dt30=1 and t1.14t16=1 then t1.pv end) dt30_14t16_pv,--近30天的14到16点的pv数量
sum(case when t1.dt30=1 and t1.17t19=1 then t1.pv end) dt30_17t19_pv,--近30天的17到19点的pv数量
sum(case when t1.dt30=1 and t1.18t19=1 then t1.pv end) dt30_18t19_pv,--近30天的18到19点的pv数量
sum(case when t1.dt30=1 and t1.20t21=1 then t1.pv end) dt30_20t21_pv,--近30天的20到21点的pv数量
sum(case when t1.dt30=1 and t1.22t23=1 then t1.pv end) dt30_22t23_pv--近30天的22到23点的pv数量
from
(
select
row_number() over(distribute by user_id sort by in_time asc) rn_asc,
row_number() over(distribute by user_id sort by in_time desc) rn_desc,
user_id,
session_id,
cookie_id,
visit_os,
browser_name,
visit_ip,
province,
city,
in_time,
out_time,
stay_time,
pv,
case when in_time>=date_sub('2019-07-10',6) then 1 else 0 end as dt7,
case when in_time>=date_sub('2019-07-10',14) then 1 else 0 end as dt15,
case when in_time>=date_sub('2019-07-10',29) then 1 else 0 end as dt30,
case when in_time>=date_sub('2019-07-10',59) then 1 else 0 end as dt60,
case when in_time>=date_sub('2019-07-10',89) then 1 else 0 end as dt90,
case when hour(in_time) between 0 and 5 then 1 else 0 end 0t5,
case when hour(in_time) between 6 and 7 then 1 else 0 end 6t7,
case when hour(in_time) between 8 and 9 then 1 else 0 end 8t9,
case when hour(in_time) between 10 and 11 then 1 else 0 end 10t11,
case when hour(in_time) between 12 and 13 then 1 else 0 end 12t13,
case when hour(in_time) between 14 and 16 then 1 else 0 end 14t16,
case when hour(in_time) between 17 and 19 then 1 else 0 end 17t19,
case when hour(in_time) between 18 and 19 then 1 else 0 end 18t19,
case when hour(in_time) between 20 and 21 then 1 else 0 end 20t21,
case when hour(in_time) between 22 and 23 then 1 else 0 end 22t23
from
qfbap_dwd.dwd_user_pc_pv
) t1 group by t1.user_id
) t2 left join
(
select
user_id,
count(distinct(case when type="visit_ip" then content end)) dt30_dis_ip_count,--近30天访问使用的不同ip数量
max(case when rn=1 and type="visit_ip" then content end) dt30_max_ip,--近30天最常用的ip
count(case when type="cookie_id" then content end) dt30_sum_cookie_ip,--近30天使用的cookie的数量
max(case when rn=1 and type="cookie_id" then content end) dt30_max_cookie_ip,--近30使用最常用的cookie_id
max(case when rn=1 and type="visit_os" then content end) dt30_max_visit_os--近30天使用最常用系统
from
qfbap_dws.dws_user_visit_month1
group by user_id
) t3 on t2.user_id=t3.user_id;