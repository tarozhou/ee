#!/bin/bash

APP_PATH=/data/zhoudongyu/newdoc_explore/
cd $APP_PATH
. /etc/profile
echo T7dDQXso | kinit zhoudongyu
day_str=`date -d "-0 day" "+%Y%m%d"`
day_str1=`date -d "-1 day" "+%Y%m%d"`

focus_data="
select t1.program_id,t1.create_time
from
(
  select program_id,create_time
  from
  (
    select program_id,create_time,regexp_replace(split(create_time,' ')[0],'-','') as fctime
    from
    (
      select program_id,create_time from data_mining.migu_video_poms_data_final_result
      shere dayid = '$day_str1' and channel=0
      union all
      select program_id,create_time from data_mining.migu_video_poms_data_final_result
      shere dayid = '$day_str' and channel=0
    )t
  )t where dayid=fctime
)t1 left join
(
    SELECT program_id
    FROM data_mining.migu_video_poms_all_copy
    WHERE package_id=1002781 AND ((source='G客号' AND assist='UGC') OR source!='G客号' OR cast(duration as int) <= 20)
)t2 on t1.program_id=t2.program_id
where t2.program_id is null group by t1.program_id,t1.create_time
"
echo $focus_data

hive -e "$focus_data" > ./data/newdoc_v1.txt

/data/zhoudongyu/pyenv27/bin/python newdoc_explore_producter.py ./data/newdoc_v1.txt

