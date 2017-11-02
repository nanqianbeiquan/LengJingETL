insert overwrite table temp.t_company_person
SELECT enterpriseName mc,
if(legalrepresentative is not null and lower(legalrepresentative) not in('','null'),legalrepresentative,
if(principal is not null and lower(principal) not in ('','null'),principal,
if(investor is not null and lower(investor) not in ('','null'),investor,
if(operator is not null and lower(operator) not in ('','null'),operator,'null'
)))) xm 
FROM gongshang.graph_app_gs_base where enterpriseName !="" and 
if(legalrepresentative is not null and lower(legalrepresentative) not in('','null'),legalrepresentative,
if(principal is not null and lower(principal) not in ('','null'),principal,
if(investor is not null and lower(investor) not in ('','null'),investor,
if(operator is not null and lower(operator) not in ('','null'),operator,'null'
)))) !='null' 

union all
select enterpriseName mc,keyperson_name xm from gongshang.graph_app_gs_baxx
join 

 where mc !=""
union all
select enterpriseName mc,shareholder_name xm from gongshang.graph_app_gs_gd
where detectshareholdertype(gdlx,zjlx,gdmc)='person' and enterpriseName !=""
;

insert overwrite table temp.t_company_company
select if(enterprisename>=shareholder_name,enterprisename,shareholder_name)
,if(enterprisename>=shareholder_name,shareholder_name,enterprisename) 
from temp.t_pachong_shareholder_info_03
where DetectShareholderType(shareholder_type,shareholder_certificationtype,shareholder_name)='company' 
and enterprisename!="" and shareholder_name!="";

create table temp.t_company_company_person
(companyName1 string
,companyName2 string
,personName string
)


insert overwrite table temp.t_company_company_person
select distinct b.companyName1,b.companyName2,a.xm
from graph_data.person_node_id a
join temp.t_company_company b
on a.glmc=b.companyName1
join graph_data.person_node_id c
on b.companyName2=c.glmc
and a.xm=c.xm


insert overwrite table temp.t_company_company_person
select distinct b.companyName1,b.companyName2,a.personName
from temp.t_company_person a
join temp.t_company_company b
on a.companyName=b.companyName1
join temp.t_company_person c
on b.companyName2=c.companyName
and a.personName=c.personName
