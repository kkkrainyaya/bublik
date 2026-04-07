create table public.offer (
    id bigint,
    open_date timestamp,
    primary key (id));

insert into public.offer (id, open_date) values (12345, '2025-06-04 00:00:00.000 +0300');
insert into public.offer (id, open_date) values (23456, '2025-06-04 00:00:00.000 +0300');

analyze public.offer;
