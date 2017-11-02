-- set @today=curdate();
-- set @yesterday=date_sub(curdate(), INTERVAL 1 DAY);
-- set @tomorrow=date_sub(curdate(), INTERVAL -1 DAY);
-- set @start_dt=@yesterday;
-- set @stop_dt=@today;
-- 北京
select 
`bjcourt`.`fa_yuan` fa_yuan_ming_cheng
,`bjcourt`.`gong_gao_nei_rong` content
,'北京' area
from `bjcourt` 
where add_time >=@start_dt and add_time<@stop_dt and gong_gao_nei_rong!='' and gong_gao_nei_rong not like '%现取消开庭。'