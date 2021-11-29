-- The core data of channel 
select t1.dt,t1.groupid,t1.groupname,t1.user_num,join_num ,out_num,act_num , chat_num, chat_pv,avg_chat_pv,avg_act_time,nvl(round(act_num/user_num,2) ,0) act_rate  from 
(select  groupid,groupname,substr(CAST(ds AS string),1,8) dt,count(distinct uid ) user_num from beacon_olap.t_updw_ods_kk_group_user_info_base_h
where substr(CAST(ds AS string),1,8)  >=cast(substring(from_unixtime(unix_timestamp(now() - interval 30 day), 'yyyyMMdd'),1,8)  AS string)

AND groupid IN (
'100525'
)
--and substr(CAST(ds AS string),9,2)='23'
and is_bot='0'
group by groupid,groupname,substr(CAST(ds AS string),1,8)) t1 
left join 
-- the number of joining the channel 
(select groupid,substr(CAST(ds AS string),1,8) dt,count(distinct uid) join_num from beacon_olap.ieg_gameplus_gameplus_user_action_report_kk
where substr(CAST(ds AS string),1,8)  >=cast(substring(from_unixtime(unix_timestamp(now() - interval 30 day), 'yyyyMMdd'),1,8)  AS string)

and operid in  (
'1103000120601')
group by groupid,substr(CAST(ds AS string),1,8) 
) t2 
on t1.dt=t2.dt 
and t1.groupid=t2.groupid 
left join 
-- the number of quiting the channel 
(select groupid,substr(CAST(ds AS string),1,8) dt,count(distinct uid) out_num from beacon_olap.ieg_gameplus_gameplus_user_action_report_kk
where substr(CAST(ds AS string),1,8)  >=cast(substring(from_unixtime(unix_timestamp(now() - interval 30 day), 'yyyyMMdd'),1,8)  AS string)

and operid in  (
'1105000710601')
group by groupid,substr(CAST(ds AS string),1,8) ) t3 
on t1.dt=t3.dt 
and t1.groupid=t3.groupid 
left join 

-- the number of active user in the channel  
(select dt,groupid,count(distinct uid) act_num from 
(select substr(CAST(ds AS string),1,8) dt ,groupid,uid  from beacon_olap.ieg_gameplus_gameplus_user_action_report_kk
where substr(CAST(ds AS string),1,8)  >=cast(substring(from_unixtime(unix_timestamp(now() - interval 30 day), 'yyyyMMdd'),1,8)  AS string)

and operid in  (
'1102000110101'
)
union all 
select substr(CAST(ds AS string),1,8)  dt ,groupid,uid   from 
beacon_olap.ieg_gameplus_gameplus_noknok_group_im_msg_kk
where substr(CAST(ds AS string),1,8)  >=cast(substring(from_unixtime(unix_timestamp(now() - interval 30 day), 'yyyyMMdd'),1,8)  AS string)

and user_type=1
and opertype in ('1','2','3')
and uid<>'0'
) t 
group by dt,groupid) t4  
on t1.dt=t4.dt 
and t1.groupid=t4.groupid 
left join 

-- the number of user who communicate with others in the channel 
(select substr(CAST(ds AS string),1,8)  dt ,groupid,count(distinct uid) chat_num ,count(distinct messageid)  chat_pv,   
round(count(distinct messageid)/count(distinct uid),2)avg_chat_pv
from 
beacon_olap.ieg_gameplus_gameplus_noknok_group_im_msg_kk
where substr(CAST(ds AS string),1,8)  >=cast(substring(from_unixtime(unix_timestamp(now() - interval 30 day), 'yyyyMMdd'),1,8)  AS string)

and user_type=1
and opertype in ('1','2','3')
and uid<>'0'
group by substr(CAST(ds AS string),1,8) ,groupid) t5 
on t1.dt=t5.dt 
and t1.groupid=t5.groupid 
left join 
-- the average active time per user
(select substr(CAST(ds AS string),1,8) dt,groupid,round(sum(cast(duration as int))/count(distinct uid)/60,4) avg_act_time from beacon_olap.ieg_gameplus_gameplus_user_action_report_kk
where substr(CAST(ds AS string),1,8)  >=cast(substring(from_unixtime(unix_timestamp(now() - interval 30 day), 'yyyyMMdd'),1,8)  AS string)

and substr(operid,1,2)='11'
and substr(operid,-4,2)='03'
and operid<>'1101000110301'
and uid<>'0'
and duration<3600*4
and groupid<>'0'
group by substr(CAST(ds AS string),1,8) ,groupid) t6 
on t1.dt=t6.dt 
and t1.groupid=t6.groupid 
order by t1.dt desc, t1.user_num desc 