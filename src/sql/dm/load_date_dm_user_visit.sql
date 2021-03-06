--------------------------------------------------------------------------------------------------------------------
pc端
--------------------------------------------------------------------------------------------------------------------
select
t2.user_id cp_user_id,
t2.recently_time cp_recently_time,
t2.recently_session cp_recently_session,
t2.recently_cookie cp_recently_cookie,
t2.recently_pv cp_recently_pv,
t2.recently_browser_name cp_recently_browser_name,
t2.recently_visit_os cp_recently_visit_os,
t2.recently_visit_ip cp_recently_visit_ip,
t2.recently_province cp_recently_province,
t2.recently_city cp_recently_city,
t2.first_time cp_first_time,
t2.first_session cp_first_session,
t2.first_cookie cp_first_cookie,
t2.first_pv cp_first_pv,
t2.first_browser_name cp_first_browser_name,
t2.first_visit_os cp_first_visit_os,
t2.first_visit_ip cp_first_visit_ip,
t2.first_province cp_first_province,
t2.first_city cp_first_city,
t2.dt7_count cp_dt7_count,
t2.dt15_count cp_dt15_count,
t2.dt30_count cp_dt30_count,
t2.dt60_count cp_dt60_count,
t2.dt90_count cp_dt90_count,
t2.dt30_pc_day cp_dt30_pc_day,
t2.dt30_pv_count cp_dt30_pv_count,
t2.dt30_avg_pv cp_dt30_avg_pv,
t2.dt30_0t5_pv cp_dt30_0t5_pv,
t2.dt30_6t7_pv cp_dt30_6t7_pv,
t2.dt30_8t9_pv cp_dt30_8t9_pv,
t2.dt30_10t11_pv cp_dt30_10t11_pv,
t2.dt30_12t13_pv cp_dt30_12t13_pv,
t2.dt30_14t16_pv cp_dt30_14t16_pv,
t2.dt30_17t19_pv cp_dt30_17t19_pv,
t2.dt30_18t19_pv cp_dt30_18t19_pv,
t2.dt30_20t21_pv cp_dt30_20t21_pv,
t2.dt30_22t23_pv cp_dt30_22t23_pv,
t3.dt30_dis_ip_count cp_dt30_dis_ip_count,
t3.dt30_max_ip cp_dt30_max_ip,
t3.dt30_sum_cookie_ip cp_dt30_sum_cookie_ip,
t3.dt30_max_cookie_ip cp_dt30_max_cookie_ip,
t3.dt30_max_visit_os cp_dt30_max_visit_os
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
max(case when t1.rn_desc=1 then t1.visit_ip end) recently_visit_ip,--最近一次使用的ip
max(case when t1.rn_desc=1 then t1.province end) recently_province,--最近一次省份
max(case when t1.rn_desc=1 then t1.city end) recently_city,--最近一次城市
max(case when t1.rn_asc=1 then t1.in_time end) first_time,--第一次pc端访问的日期
max(case when t1.rn_asc=1 then t1.session_id end) first_session,--第一次pc端访问的session
max(case when t1.rn_asc=1 then t1.cookie_id end) first_cookie,--第一次pc端访问的cookie
max(case when t1.rn_asc=1 then t1.pv end) first_pv,--第一次访问的pv
max(case when t1.rn_asc=1 then t1.browser_name end) first_browser_name,--第一次访问使用的浏览器
max(case when t1.rn_asc=1 then t1.visit_os end) first_visit_os,--第一次访问的os
max(case when t1.rn_asc=1 then t1.visit_ip end) first_visit_ip,--最近一次使用的ip
max(case when t1.rn_asc=1 then t1.province end) first_province,--最近一次省份
max(case when t1.rn_asc=1 then t1.city end) first_city,--最近一次城市
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

--------------------------------------------------------------------------------------------------------------------
app端
--------------------------------------------------------------------------------------------------------------------
select
t4.user_id  user_id,
max(case when rn_desc=1 then log_time end) last_log_time,
max(case when rn_desc=1 then app_name end) last_app_name,
max(case when rn_desc=1 then os_version end) last_os,
max(case when rn_desc=1 then visit_ip end) last_visit_ip,
max(case when rn_desc=1 then province end) last_province,
max(case when rn_desc=1 then city end) last_city,
max(case when rn_asc=1 then log_time end) first_log_time,
max(case when rn_asc=1 then app_name end) first_app_name,
max(case when rn_asc=1 then os_version end) first_os,
max(case when rn_asc=1 then visit_ip end) first_visit_ip,
max(case when rn_asc=1 then province end) first_province,
max(case when rn_asc=1 then city end) first_city,
sum(t4.dt7) dt7_count,--PC连续7天访问次数
sum(t4.dt15) dt15_count,--连续15天访问次数
sum(t4.dt30) dt30_count,--连续30天访问次数
sum(t4.dt60) dt60_count,--连续60天访问的次数
sum(t4.dt90) dt90_count,--	连续90天访问的次数
sum(case when t4.dt30=1 and t4.0t5=1 then 1 else 0 end) dt30_0t5_pv,--	近30天的0到5点的pv数量
sum(case when t4.dt30=1 and t4.6t7=1 then 1 else 0 end) dt30_6t7_pv,--近30天的6到7点的pv数量
sum(case when t4.dt30=1 and t4.8t9=1 then 1 else 0 end) dt30_8t9_pv,--近30天的8到9的pv数量
sum(case when t4.dt30=1 and t4.10t11=1 then 1 else 0 end) dt30_10t11_pv,--近30天的10到11的pv数量
sum(case when t4.dt30=1 and t4.12t13=1 then 1 else 0 end) dt30_12t13_pv,--近30天的12到13的pv数量
sum(case when t4.dt30=1 and t4.14t15=1 then 1 else 0 end) dt30_14t15_pv,--近30天的14到16点的pv数量
sum(case when t4.dt30=1 and t4.16t17=1 then 1 else 0 end) dt30_16t17_pv,--近30天的17到19点的pv数量
sum(case when t4.dt30=1 and t4.18t19=1 then 1 else 0 end) dt30_18t19_pv,--近30天的18到19点的pv数量
sum(case when t4.dt30=1 and t4.20t21=1 then 1 else 0 end) dt30_20t21_pv,--近30天的20到21点的pv数量
sum(case when t4.dt30=1 and t4.22t23=1 then 1 else 0 end) dt30_22t23_pv--近30天的22到23点的pv数量
from
(
select
user_id,
row_number() over(distribute by user_id sort by log_time asc) rn_asc,
row_number() over(distribute by user_id sort by log_time desc) rn_desc,
app_name,
os_version,
log_time,
visit_ip,
province,
city,
case when log_time>=date_sub('2019-07-10',6) then 1 else 0 end as dt7,
case when log_time>=date_sub('2019-07-10',14) then 1 else 0 end as dt15,
case when log_time>=date_sub('2019-07-10',29) then 1 else 0 end as dt30,
case when log_time>=date_sub('2019-07-10',59) then 1 else 0 end as dt60,
case when log_time>=date_sub('2019-07-10',89) then 1 else 0 end as dt90,
case when hour(log_time) between 0 and 5 then 1 else 0 end 0t5,
case when hour(log_time) between 6 and 7 then 1 else 0 end 6t7,
case when hour(log_time) between 8 and 9 then 1 else 0 end 8t9,
case when hour(log_time) between 10 and 11 then 1 else 0 end 10t11,
case when hour(log_time) between 12 and 13 then 1 else 0 end 12t13,
case when hour(log_time) between 14 and 15 then 1 else 0 end 14t15,
case when hour(log_time) between 16 and 17 then 1 else 0 end 16t17,
case when hour(log_time) between 18 and 19 then 1 else 0 end 18t19,
case when hour(log_time) between 20 and 21 then 1 else 0 end 20t21,
case when hour(log_time) between 22 and 23 then 1 else 0 end 22t23
from
qfbap_dwd.dwd_user_app_pv
) t4
group by t4.user_id;

--------------------------------------------------------------------------------------------------------------------
综合指标
--------------------------------------------------------------------------------------------------------------------

select
t5.user_id,
t5.cp_recently_time,
t5.cp_recently_session,
t5.cp_recently_cookie,
t5.cp_recently_pv,
t5.cp_recently_browser_name,
t5.cp_recently_visit_os,
t5.cp_recently_visit_ip,
t5.cp_recently_province,
t5.cp_recently_city,
t5.cp_first_time,
t5.cp_first_session,
t5.cp_first_cookie,
t5.cp_first_pv,
t5.cp_first_browser_name,
t5.cp_first_visit_os,
t5.cp_first_visit_ip,
t5.cp_first_province,
t5.cp_first_city,
t5.cp_dt7_count,
t5.cp_dt15_count,
t5.cp_dt30_count,
t5.cp_dt60_count,
t5.cp_dt90_count,
t5.cp_dt30_pc_day,
t5.cp_dt30_pv_count,
t5.cp_dt30_avg_pv,
t5.cp_dt30_0t5_pv,
t5.cp_dt30_6t7_pv,
t5.cp_dt30_8t9_pv,
t5.cp_dt30_10t11_pv,
t5.cp_dt30_12t13_pv,
t5.cp_dt30_14t16_pv,
t5.cp_dt30_17t19_pv,
t5.cp_dt30_18t19_pv,
t5.cp_dt30_20t21_pv,
t5.cp_dt30_22t23_pv,
t5.cp_dt30_dis_ip_count,
t5.cp_dt30_max_ip,
t5.cp_dt30_sum_cookie_ip,
t5.cp_dt30_max_cookie_ip,
t5.cp_dt30_max_visit_os,
t6.last_log_time,
t6.last_app_name,
t6.last_os,
t6.last_visit_ip,
t6.last_province,
t6.last_city,
t6.first_log_time,
t6.first_app_name,
t6.first_os,
t6.first_visit_ip,
t6.first_province,
t6.first_city,
t6.dt7_count,
t6.dt15_count,
t6.dt30_count,
t6.dt60_count,
t6.dt90_count,
t6.dt30_0t5_pv,
t6.dt30_6t7_pv,
t6.dt30_8t9_pv,
t6.dt30_10t11_pv,
t6.dt30_12t13_pv,
t6.dt30_14t15_pv,
t6.dt30_16t17_pv,
t6.dt30_18t19_pv,
t6.dt30_20t21_pv,
t6.dt30_22t23_pv,
case when t5.cp_recently_time>t6.last_log_time then t5.cp_recently_visit_ip else t6.last_visit_ip end recently_ip,
case when t5.cp_recently_time>t6.last_log_time then t5.cp_recently_province else t6.last_province end recently_province,
case when t5.cp_recently_time>t6.last_log_time then t5.cp_recently_city else t6.last_city end recently_city,
case when t5.cp_first_time<t6.first_log_time then t5.cp_first_visit_ip else t6.first_visit_ip end
first_ip,
case when t5.cp_first_time<t6.first_log_time then t5.cp_first_province else t6.first_province end
first_province,
case when t5.cp_first_time<t6.first_log_time then t5.cp_first_city else t6.first_city end
first_city
from
(
select
t2.user_id user_id,
t2.recently_time cp_recently_time,
t2.recently_session cp_recently_session,
t2.recently_cookie cp_recently_cookie,
t2.recently_pv cp_recently_pv,
t2.recently_browser_name cp_recently_browser_name,
t2.recently_visit_os cp_recently_visit_os,
t2.recently_visit_ip cp_recently_visit_ip,
t2.recently_province cp_recently_province,
t2.recently_city cp_recently_city,
t2.first_time cp_first_time,
t2.first_session cp_first_session,
t2.first_cookie cp_first_cookie,
t2.first_pv cp_first_pv,
t2.first_browser_name cp_first_browser_name,
t2.first_visit_os cp_first_visit_os,
t2.first_visit_ip cp_first_visit_ip,
t2.first_province cp_first_province,
t2.first_city cp_first_city,
t2.dt7_count cp_dt7_count,
t2.dt15_count cp_dt15_count,
t2.dt30_count cp_dt30_count,
t2.dt60_count cp_dt60_count,
t2.dt90_count cp_dt90_count,
t2.dt30_pc_day cp_dt30_pc_day,
t2.dt30_pv_count cp_dt30_pv_count,
t2.dt30_avg_pv cp_dt30_avg_pv,
t2.dt30_0t5_pv cp_dt30_0t5_pv,
t2.dt30_6t7_pv cp_dt30_6t7_pv,
t2.dt30_8t9_pv cp_dt30_8t9_pv,
t2.dt30_10t11_pv cp_dt30_10t11_pv,
t2.dt30_12t13_pv cp_dt30_12t13_pv,
t2.dt30_14t16_pv cp_dt30_14t16_pv,
t2.dt30_17t19_pv cp_dt30_17t19_pv,
t2.dt30_18t19_pv cp_dt30_18t19_pv,
t2.dt30_20t21_pv cp_dt30_20t21_pv,
t2.dt30_22t23_pv cp_dt30_22t23_pv,
t3.dt30_dis_ip_count cp_dt30_dis_ip_count,
t3.dt30_max_ip cp_dt30_max_ip,
t3.dt30_sum_cookie_ip cp_dt30_sum_cookie_ip,
t3.dt30_max_cookie_ip cp_dt30_max_cookie_ip,
t3.dt30_max_visit_os cp_dt30_max_visit_os
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
max(case when t1.rn_desc=1 then t1.visit_ip end) recently_visit_ip,--最近一次使用的ip
max(case when t1.rn_desc=1 then t1.province end) recently_province,--最近一次省份
max(case when t1.rn_desc=1 then t1.city end) recently_city,--最近一次城市
max(case when t1.rn_asc=1 then t1.in_time end) first_time,--第一次pc端访问的日期
max(case when t1.rn_asc=1 then t1.session_id end) first_session,--第一次pc端访问的session
max(case when t1.rn_asc=1 then t1.cookie_id end) first_cookie,--第一次pc端访问的cookie
max(case when t1.rn_asc=1 then t1.pv end) first_pv,--第一次访问的pv
max(case when t1.rn_asc=1 then t1.browser_name end) first_browser_name,--第一次访问使用的浏览器
max(case when t1.rn_asc=1 then t1.visit_os end) first_visit_os,--第一次访问的os
max(case when t1.rn_asc=1 then t1.visit_ip end) first_visit_ip,--最近一次使用的ip
max(case when t1.rn_asc=1 then t1.province end) first_province,--最近一次省份
max(case when t1.rn_asc=1 then t1.city end) first_city,--最近一次城市
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
) t3 on t2.user_id=t3.user_id
) t5
left join
(
select
t4.user_id  user_id,
max(case when rn_desc=1 then log_time end) last_log_time,
max(case when rn_desc=1 then app_name end) last_app_name,
max(case when rn_desc=1 then os_version end) last_os,
max(case when rn_desc=1 then visit_ip end) last_visit_ip,
max(case when rn_desc=1 then province end) last_province,
max(case when rn_desc=1 then city end) last_city,
max(case when rn_asc=1 then log_time end) first_log_time,
max(case when rn_asc=1 then app_name end) first_app_name,
max(case when rn_asc=1 then os_version end) first_os,
max(case when rn_asc=1 then visit_ip end) first_visit_ip,
max(case when rn_asc=1 then province end) first_province,
max(case when rn_asc=1 then city end) first_city,
sum(t4.dt7) dt7_count,--PC连续7天访问次数
sum(t4.dt15) dt15_count,--连续15天访问次数
sum(t4.dt30) dt30_count,--连续30天访问次数
sum(t4.dt60) dt60_count,--连续60天访问的次数
sum(t4.dt90) dt90_count,--	连续90天访问的次数
sum(case when t4.dt30=1 and t4.0t5=1 then 1 else 0 end) dt30_0t5_pv,--	近30天的0到5点的pv数量
sum(case when t4.dt30=1 and t4.6t7=1 then 1 else 0 end) dt30_6t7_pv,--近30天的6到7点的pv数量
sum(case when t4.dt30=1 and t4.8t9=1 then 1 else 0 end) dt30_8t9_pv,--近30天的8到9的pv数量
sum(case when t4.dt30=1 and t4.10t11=1 then 1 else 0 end) dt30_10t11_pv,--近30天的10到11的pv数量
sum(case when t4.dt30=1 and t4.12t13=1 then 1 else 0 end) dt30_12t13_pv,--近30天的12到13的pv数量
sum(case when t4.dt30=1 and t4.14t15=1 then 1 else 0 end) dt30_14t15_pv,--近30天的14到16点的pv数量
sum(case when t4.dt30=1 and t4.16t17=1 then 1 else 0 end) dt30_16t17_pv,--近30天的17到19点的pv数量
sum(case when t4.dt30=1 and t4.18t19=1 then 1 else 0 end) dt30_18t19_pv,--近30天的18到19点的pv数量
sum(case when t4.dt30=1 and t4.20t21=1 then 1 else 0 end) dt30_20t21_pv,--近30天的20到21点的pv数量
sum(case when t4.dt30=1 and t4.22t23=1 then 1 else 0 end) dt30_22t23_pv--近30天的22到23点的pv数量
from
(
select
user_id,
row_number() over(distribute by user_id sort by log_time asc) rn_asc,
row_number() over(distribute by user_id sort by log_time desc) rn_desc,
app_name,
os_version,
log_time,
visit_ip,
province,
city,
case when log_time>=date_sub('2019-07-10',6) then 1 else 0 end as dt7,
case when log_time>=date_sub('2019-07-10',14) then 1 else 0 end as dt15,
case when log_time>=date_sub('2019-07-10',29) then 1 else 0 end as dt30,
case when log_time>=date_sub('2019-07-10',59) then 1 else 0 end as dt60,
case when log_time>=date_sub('2019-07-10',89) then 1 else 0 end as dt90,
case when hour(log_time) between 0 and 5 then 1 else 0 end 0t5,
case when hour(log_time) between 6 and 7 then 1 else 0 end 6t7,
case when hour(log_time) between 8 and 9 then 1 else 0 end 8t9,
case when hour(log_time) between 10 and 11 then 1 else 0 end 10t11,
case when hour(log_time) between 12 and 13 then 1 else 0 end 12t13,
case when hour(log_time) between 14 and 15 then 1 else 0 end 14t15,
case when hour(log_time) between 16 and 17 then 1 else 0 end 16t17,
case when hour(log_time) between 18 and 19 then 1 else 0 end 18t19,
case when hour(log_time) between 20 and 21 then 1 else 0 end 20t21,
case when hour(log_time) between 22 and 23 then 1 else 0 end 22t23
from
qfbap_dwd.dwd_user_app_pv
) t4
group by t4.user_id
) t6
on
t5.user_id=t6.user_id;