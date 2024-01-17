-- Jess Ramos
-- Solve Real-World Data Problems with SQL

-- consecutive timestamp differences
select
    *,
    lead(movementdate, 1) over(partition by subscriptionid order by movementdate) as nextstatusmovementdate,
    lead(movementdate, 1) over(partition by subscriptionid order by movementdate)
    - movementdate as timeinstatus
from
    paymentstatuslog
where
    subscriptionid = '38844';

-- tracking running totals with window functions
select 
    salesemployeeid,
    saledate,
    saleamount,
    sum(saleamount) over(partition by salesemployeeid order by saledate) as running_total,
    cast(sum(saleamount) over(partition by salesemployeeid order by saledate) as float)
    / 
    quota as percent_quota
from
    sales
join 
    employees e
on salesemployeeid = e.employeeid
order by salesemployeeid;

-- using self join to compare rows within the same table
with monthly_revenue as 
(
select 
    date_trunc('month', orderdate) as order_month, 
    sum(revenue) as monthly_revenue
from 
    subscriptions
group by 
    date_trunc('month', orderdate)
)

select curr.order_month as current_month,
        prev.order_month as previous_month,
        curr.monthly_revenue as current_revenue,
        prev.monthly_revenue as previous_revenue
from
    monthly_revenue curr
left join 
    monthly_revenue prev
where curr.monthly_revenue > prev.monthly_revenue
    and DATEDIFF(month, prev.order_month, curr.order_month) = 1;

-- using self joins to pull hierarchical relationships
select
    e.employeeid,
    e.name as employee_name,
    m.name as manager_name,
    coalesce(m.email, e.email) as contact_email
from 
    employees e
left join employees m
on e.managerid = m.employeeid
where e.department = 'Sales';

-- unpivoting columns into rows using union
with all_cancelation_reasons as(
select 
    subscriptionid,
    cancelationreason1 as cancelationreason
FROM 
    cancelations
union 
select 
    subscriptionid,
    cancelationreason2 as cancelationreason
FROM 
    cancelations
union
select 
    subscriptionid,
    cancelationreason3 as cancelationreason
FROM 
    cancelations
)

select 
    cast(count(
        case when cancelationreason = 'Expensive' 
        then subscriptionid end) as float)
    /count(distinct subscriptionid) as percent_expensive
from    
    all_cancelation_reasons
;

-- combining two product table into one
With all_subscriptions as(
select *
from subscriptionsproduct1 s1
where s1.active = 1
union 
select *
from subscriptionsproduct2 s2
where s2.active = 1
)
select
	date_trunc('year', expirationdate) as exp_year, 
	count(*) as subscriptions
from 
	all_subscriptions
group by 
	date_trunc('year', expirationdate);

-- pivotin rows into aggregated columns with CASE
select 
	userid,
	sum(case when elog.eventid = 1 then 1 else 0 end) as VIEWEDHELPCENTERPAGE,
	sum(case when elog.eventid = 2 then 1 else 0 end) as CLICKEDFAQS,
	sum(case when elog.eventid = 3 then 1 else 0 end) as CLICKEDCONTACTSUPPORT,
	sum(case when elog.eventid = 4 then 1 else 0 end) as SUBMITTEDTICKET
from
	frontendeventlog elog
join 
	frontendeventdefinitions def
	on elog.eventid = def.eventid
where 
	eventtype = 'Customer Support'
group by 
	userid;

-- calculating descriptive statistics for monthly revenue by product
with monthly_rev as(
select 
    p.productname, 
    sum(s.revenue) revenue, 
    month(s.OrderDate) as mon
from subscriptions s
inner join products p
on p.productid = s.productid
where year(s.OrderDate) = '2022'
group by mon, productname
)

select 
    productname, 
    min(revenue) min_rev, 
    max(revenue) max_rev, 
    avg(revenue) avg_rev,
    stddev(revenue) std_dev_rev
from monthly_rev
group by productname;

-- exploring variable distribution with CTEs
with link_clicks as (
select 
    count(fl.eventlogid) num_link_clicks, 
    fl.userid
from frontendeventlog fl
where fl.EVENTID = 5
group by fl.USERID
)

select 
    num_link_clicks, 
    count(lc.userid) num_users
from link_clicks lc
group by num_link_clicks;

-- Payment funnel analysis with multiple CTE
with max_status as (
select 
	subscriptionid, 
	max(statusid) maxstatus
from paymentstatuslog
group by subscriptionid
),

currstatus as (
select 
	s.subscriptionid, 
	s.currentstatus, 
	maxstatus
from subscriptions s
left join max_status m
on s.subscriptionid = m.subscriptionid
)

select
case when maxstatus = 1 then 'PaymentWidgetOpened'
		when maxstatus = 2 then 'PaymentEntered'
		when maxstatus = 3 and currentstatus = 0 then 'User Error with Payment Submission'
		when maxstatus = 3 and currentstatus != 0 then 'Payment Submitted'
		when maxstatus = 4 and currentstatus = 0 then 'Payment Processing Error with Vendor'
		when maxstatus = 4 and currentstatus != 0 then 'Payment Success'
		when maxstatus = 5 then 'Complete'
		when maxstatus is null then 'User did not start payment process'
		end as paymentfunnelstage,
	count(subscriptionid) as subscriptions
from currstatus
group by paymentfunnelstage;

-- creating binary columns with case
with counting as (
select
    customerid,
    count(PRODUCTID) num_products,
    sum(NUMBEROFUSERS) total_users
from subscriptions
group by customerid
)

select *,
    case 
        when total_users > 5000 or num_products = 1 then 1
        else 0
    end upsell_opportunity
from counting;


select eventid,
    case 
    when description = 'viewedhelpcenterpage' then 1
    when description = 'clickedfaqs' then 2
    when description = 'clickedcontactsupport' then 3
    when description = 'submittedticket' then 4
    end event_status
from frontendeventdefinitions
where eventtype = 'customer support'
group by eventid;
