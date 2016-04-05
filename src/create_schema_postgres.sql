drop table if exists EventSubscriptionActivity;
drop table if exists EventSubscription;
drop table if exists SubscriptionTopic;
drop table if exists SubscriptionRevision;
drop table if exists Subscription;
drop table if exists EventMutexReleased;
drop table if exists EventMutex;
drop table if exists "Event";

create table "Event" (
	EventId bigserial primary key not null,
	EventData varchar,
	TopicData jsonb not null,
	TopicCount int not null,
	CreateDate timestamp default current_timestamp not null,
	CreatedByHostName varchar(100),
	CreatedByUser varchar(100) not null,
	CreatedByApplication varchar(100) default 'System' not null
);

create index IX_TOPICDATA_GIN on "Event" using gin (TopicData);

create table EventMutex (
	EventMutexId bigserial primary key not null,
	"Key" varchar(100) not null,
	Hash bigint not null,
	Expiration timestamp not null,
	CreateDate timestamp default current_timestamp not null,
	CreatedByHostName varchar(100),
	CreatedByUser varchar(100) not null,
	CreatedByApplication varchar(100) default 'System' not null
);

create table EventMutexReleased (
	EventMutexReleasedId bigserial primary key not null,
	EventMutexId bigint not null references EventMutex(EventMutexId),
	CreateDate timestamp default current_timestamp not null,
	CreatedByHostName varchar(100),
	CreatedByUser varchar(100) not null,
	CreatedByApplication varchar(100) default 'System' not null
);

create table Subscription (
	SubscriptionId serial primary key not null,
	CurrentSubscriptionRevisionId int not null,
	CreateDate timestamp default current_timestamp not null,
	CreatedByHostName varchar(100),
	CreatedByUser varchar(100) not null,
	CreatedByApplication varchar(100) default 'System' not null
);

create table SubscriptionRevision (
	SubscriptionRevisionId serial primary key not null,
	SubscriptionId int references Subscription(SubscriptionId) not null,
	SubscriptionName varchar(100) not null,
	SubscriptionStatusCode varchar(20) not null,
	ServiceEndpoint varchar(1000) not null,
	ServiceTypeCode varchar(100) not null,
	HTTPMethod varchar(10) not null,
	TransformFunction varchar null,
	AbortAfterMinutes int not null,
	EscalationConfiguration varchar null,	
	RequestType varchar(15),
	CreateDate timestamp default current_timestamp not null,
	CreatedByHostName varchar(100),
	CreatedByUser varchar(100) not null,
	CreatedByApplication varchar(100) default 'System' not null
);

create table SubscriptionTopic (
	SubscriptionTopicId serial primary key not null,
	SubscriptionRevisionId int references SubscriptionRevision(SubscriptionRevisionId) not null,
	"Key" varchar(100) not null,
	"Value" varchar(100) not null,
	OperatorTypeCode varchar(100) not null,
	CreateDate timestamp default current_timestamp not null,
	CreatedByHostName varchar(100),
	CreatedByUser varchar(100) not null,
	CreatedByApplication varchar(100) default 'System' not null
);

create table EventSubscription (
	EventSubscriptionId bigserial primary key not null,
	EventId bigint references "Event"(EventId) not null,
	SubscriptionId int references Subscription(SubscriptionId) not null,
	CreateDate timestamp default current_timestamp not null,
	CreatedByHostName varchar(100),
	CreatedByUser varchar(100) not null,
	CreatedByApplication varchar(100) default 'System' not null
);

create table EventSubscriptionActivity (
	EventSubscriptionActivityId bigserial primary key not null,
	ActivityTypeCode varchar(100) not null,
	EventId bigint references "Event"(EventId) not null,
	EventSubscriptionId bigint references EventSubscription(EventSubscriptionId) null,
	"Data" varchar,
	CreateDate timestamp default current_timestamp not null,
	CreatedByHostName varchar(100),
	CreatedByUser varchar(100) not null,
	CreatedByApplication varchar(100) default 'System' not null
);

drop function if exists h_bigint(text);
create function h_bigint(text) returns bigint as 
$$
select ('x' || substr(md5($1), 1, 16))::bit(64)::bigint;
$$
language sql;

drop function if exists TryAcquireMutex(text, timestamp, text, text);
create function TryAcquireMutex(text, timestamp, text, text) returns bigint as
$$
declare 
	hashVal bigint := h_bigint($1);
	returnVal bigint;
begin
	perform pg_advisory_lock(hashVal);
	if not exists (select 1
		from EventMutex em
		left join EventMutexReleased emr
			on emr.EventMutexId = em.EventMutexId
		where em.Hash = hashVal
			and Expiration > current_timestamp
			and emr.EventMutexReleasedId is null
		limit 1)
	then
		insert into EventMutex
			("Key",
			"hash",
			Expiration,
			CreatedByUser,
			CreatedByApplication)
		values 
			($1,
			hashVal,
			$2,
			$3,
			$4);
		returnVal := (select lastval());	
	end if;
	perform pg_advisory_unlock(hashVal);
	return returnVal;
end
$$
language plpgsql;