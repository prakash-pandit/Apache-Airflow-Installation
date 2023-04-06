insert into CVM_STG.cvm_lec_inventoryfinal
with 
	trans_inventory as (
		select 
	cast(transi_lgnnumber as integer) as Transi_lgnnumber ,
	Cinoperator_strcode,
	Item_stritemid ,
	cast(Transi_dtmdatetime as datetime) ,
	sum(Transi_decnoofitems) as transi_decnoofitems, 
	cast(promotiond_intid as integer) as promotiond_intid,
	sum(case 
			when Transi_decnoofitems::float < 0 then -Transi_curdiscount::float 
			else Transi_curdiscount::float
		end ) as transi_curdiscount, 
	case
		when workstation_strcode like 'K\_%' then 'Online'
		else 'Offline'
	end as Transaction_type,
	sum(case 
			when Transi_decnoofitems::float < 0 then -transi_curvalueeach::float
			else transi_curvalueeach ::float
		end ) as transi_curvalueeach
from
	VOXBI_ODS.ods_voxbi_fct_tbltrans_inventory
where
	cast(transi_dtmdatetime as date) > '2021-05-01' and transi_strType in ('S', 'R') and transi_lgnnumber is not null and cinoperator_strcode is not null
	and transi_dtmdatetime is not null and item_stritemid is not null
group by transi_lgnnumber,	cinoperator_strcode ,item_stritemid ,transi_dtmdatetime,transi_dtmdatetime ,promotiond_intid ,workstation_strcode
	),
	trans_cash as(
		select
		transc_lgnnumber ,
		cinoperator_strcode ,
		transc_dtmdatetime ,
		transc_strmemberid,
		listagg(distinct case
			when transc_strbkcardtype is null then 'cash'
			when transc_strbkcardtype is not null and transc_strbkcardtype = 'master' or transc_strbkcardtype = 'master_car' then 'mastercard'
			when transc_strbkcardtype is not null and transc_strbkcardtype = 'share_poin' or transc_strbkcardtype = 'loyal' then 'sharepoint'
			when transc_strbkcardtype is not null and transc_strbkcardtype = 'credit car' or transc_strbkcardtype = 'credit car' then 'creditcard'
			when transc_strbkcardtype is not null and transc_strbkcardtype = 'auto credi' or transc_strbkcardtype = 'auto credt' then 'auto credit'
			else transc_strbkcardtype
		end ) as payment_type
		from
			voxbi_ods.ods_voxbi_fct_tbltrans_cash
		where
			cast(transc_dtmdatetime as date) = '2022-03-05' 
			and transc_lgnnumber is not null
			and cinoperator_strcode is not null
			and transc_dtmdatetime is not null
			and transc_strmemberid is not null
			and cinoperator_strcode not like 'drp%'
		group by
			transc_lgnnumber,
			cinoperator_strcode ,
			transc_dtmdatetime,
			transc_strmemberid 
),
cinema_operator as (
		select cinema_strcode, currency_strcode,  cinoperator_strcode from voxbi_ods.ods_voxbi_dim_tblcinema_operator where active_flag = 1
),
cinema as (
		select cinema_strcode, country_strcode,
		case
			when cinema_strshortname like '%moen%' then substring(cinema_strshortname , 1, 3)
			else cinema_strshortname
		end as cinema_strshortname
		from voxbi_ods.ods_voxbi_dim_tblcinema where active_flag = 1
),
itemAll as ( 
		select hopk,item_stritemdescription,item_stritemid,locid,class_strcode from voxbi_ods.ods_voxbi_dim_tblitemallx where active_flag = 1
),
item_class as (
		select class_strcode,class_strdescription from voxbi_ods.ods_voxbi_dim_tblitem_class where active_flag = 1 
),
promotion_details as (
		select promotiond_intid,promotionh_strcode,	locid from voxbi_ods.ods_voxbi_dim_tblpromotiondetailsallx where active_flag = 1
),
promotion_header as (
		select 	hopk,promotionh_strcode,promotionh_strdescription,promotionh_strextendeddesc,locid from voxbi_ods.ods_voxbi_dim_tblpromotionheaderallx where active_flag = 1
),
country as ( 
		select country_strcode,	country_strname	from voxbi_ods.ods_voxbi_dim_tblcountry 
),
dim_member as(
		select 	gcr_id,	share_member_id from lyl_dwh.dim_member 
),
cognetic_members_card as (
		select card_cardnumber,	card_membershipid from voxbi_ods.ods_voxbi_dim_cognetic_members_card where active_flag = 1
),
cognetic_member_membership as (
		select membership_id, membership_personid from voxbi_ods.ods_voxbi_dim_cognetic_members_membership where active_flag = 1
),
dim_membership as (
		select vox_membership_hash_id ,membership_person_id from ca_ods.ods_vox_lyl_dim_membership where active_flag = 1
),
gcr_mapping as (
		select hash_id, gcr_id from ca_ods.ods_gcr_source_mapping
),
inventory_final as 
	(
select 
			concat(a.transc_lgnnumber, a.cinoperator_strcode) as transaction_data ,
			concat(a.transc_lgnnumber, a.cinoperator_strcode) as transaction_id,
			a.transc_dtmdatetime as transaction_time,
			co.cinema_strcode as transaction_locationcode,
			h.country_strname as country,
			d.hopk as product_id,
			d.item_stritemdescription as product_name,
			e.class_strdescription as product_category,
			b.transi_decnoofitems as quantity,
			co.currency_strcode as currency ,
			case
				when f.promotiond_intid is not null and b.transi_curdiscount is not null then b.transi_curdiscount + b.transi_curvalueeach
				when f.promotiond_intid is null or b.transi_curdiscount is null then b.transi_curvalueeach
			end as normal_price,
			b.transi_curvalueeach as sales_price,
			case
				when f.promotiond_intid is not null and b.transi_curdiscount is not null then  b.transi_curdiscount
				when f.promotiond_intid is null or b.transi_curdiscount is null then '0'
			end ::float as discount,
			concat(concat(g.promotionh_strdescription, ':'), g.promotionh_strextendeddesc) as discount_type,
			cast(g.hopk as integer) as promotion_code,
			a.transc_strmemberid as customer_id,
			a.payment_type,
			coalesce (i.gcr_id, m.gcr_id) as gcr_id,
			null as movie_name,
			null as movie_category,
			null::timestamp as session_start_time,
			null::timestamp as session_end_time,
			null as movie_id,
			null as language,
			b.transaction_type
	 from
		trans_cash a 
	inner join trans_inventory b on 
		a.transc_lgnnumber = b.transi_lgnnumber ::integer
		and a.cinoperator_strcode = b.cinoperator_strcode
		and a.transc_dtmdatetime = b.transi_dtmdatetime ::datetime
	left join cinema_operator co on a.cinoperator_strcode = co.cinoperator_strcode
	left join cinema c on co.cinema_strcode = c.cinema_strcode
	left join itemAll d on b.item_stritemid = d.item_stritemid and c.cinema_strshortname = d.locid
	left join item_class e on d.class_strcode = e.class_strcode
	left join promotion_details f on b.promotiond_intid = f.promotiond_intid and d.locid = f.locid
	left join promotion_header g on f.promotionh_strcode = g.promotionh_strcode and f.locid = g.locid
	left join country h on c.country_strcode = h.country_strcode
	left join dim_member i on	i.share_member_id = a.transc_strmemberid and a.transc_strmemberid like '9%'
	left join cognetic_members_card j on a.transc_strmemberid = j.card_cardnumber and a.transc_strmemberid not like '9%'
	left join cognetic_member_membership k 	on 	k.membership_id = j.card_membershipid
	left join dim_membership l on 	l.membership_person_id = k.membership_personid
	left join gcr_mapping m on 	m.hash_id = l.vox_membership_hash_id
	where product_id != 1 and product_category not like '%ingredient%' and product_name not like '%ingredient%' and product_name not like '%ingredient%'
			and product_name not like '%3d%' and d.class_strcode != 0102
) select count(*) from inventory_final;