-- Таблица-источник TTL кэша)
create table public.offer (
    id bigint,
    open_date timestamp,
    cb_service_name text,
    targeting_type text,
    close_date timestamp,
    primary key (id)
);

create table public.source (
    client_id varchar,
    offer_id bigint,
    client_type int,
    temp_aud jsonb,
    primary key (client_id)
);


-- Вставка тестовых данных в offer
-- id=12345: HOTELS_POSTPAY - живет 2 года
insert into public.offer (id, open_date, cb_service_name, targeting_type, close_date)
values (12345, '2025-06-04 00:00:00.000 +0300', 'HOTELS_POSTPAY', 'DYNAMIC', '2026-12-04 00:00:00.000 +0300');

-- id=23456: HOTELS_POSTPAY_PREDICTOR - живет 2 года
insert into public.offer (id, open_date, cb_service_name, targeting_type, close_date)
values (23456, '2025-06-04 00:00:00.000 +0300', 'HOTELS_POSTPAY_PREDICTOR', 'DYNAMIC', '2026-12-04 00:00:00.000 +0300');

-- id=34567: AVIA - живет 6 месяцев
insert into public.offer (id, open_date, cb_service_name, targeting_type, close_date)
values (34567, '2025-06-04 00:00:00.000 +0300', 'AVIA', 'DYNAMIC', '2026-12-04 00:00:00.000 +0300');

-- id=45678: CONCERT - живет 6 месяцев
insert into public.offer (id, open_date, cb_service_name, targeting_type, close_date)
values (45678, '2025-06-04 00:00:00.000 +0300', 'CONCERT', 'TRANSACTION', '2026-12-04 00:00:00.000 +0300');

-- id=56789: HEALTH - живет 1 месяц
insert into public.offer (id, open_date, cb_service_name, targeting_type, close_date)
values (56789, '2025-06-04 00:00:00.000 +0300', 'HEALTH', 'TRANSACTION', '2026-12-04 00:00:00.000 +0300');

-- id=77777: EXPIRED - для теста отрицательного TTL
insert into public.offer (id, open_date, cb_service_name, targeting_type, close_date)
values (77777, '2020-01-01 00:00:00.000 +0300', 'HEALTH', 'TRANSACTION', '2020-02-01 00:00:00.000 +0300');

-- id=88888: - для теста верхнего ограничения TTL
insert into public.offer (id, open_date, cb_service_name, targeting_type, close_date)
values (88888, '2020-01-01 00:00:00.000 +0300', 'AVIA', 'TRANSACTION', '2030-02-01 00:00:00.000 +0300');

-- Вставка тестовых данных в ttl_check
-- client_id=1: offer_id=12345 - TTL из кэша (2 года)
insert into public.source (client_id, offer_id, client_type, temp_aud) values ('1', 12345, null, '[]'::jsonb);

-- client_id=2: offer_id=23456 - TTL из кэша (2 года)
insert into public.source (client_id, offer_id, client_type, temp_aud) values ('2', 23456, null, '[{"key": "value"}]'::jsonb);

-- client_id=3: offer_id=34567 - TTL из кэша (6 месяцев)
insert into public.source (client_id, offer_id, client_type, temp_aud) values ('3', 34567, 1, '[]'::jsonb);

-- client_id=4: offer_id=45678 - TTL из кэша (6 месяцев)
insert into public.source (client_id, offer_id, client_type) values ('4', 45678, 1);

-- client_id=5: offer_id=56789 - TTL из кэша (1 месяц)
insert into public.source (client_id, offer_id, client_type) values ('5', 56789, 1);

-- client_id=6: offer_id=99999 - отсутствует в кэше, TTL по умолчанию (2 года)
insert into public.source (client_id, offer_id, client_type) values ('6', 99999, 1);

-- client_id=7: offer_id=77777 - отрицательный TTL, будет заменен на 1 неделю
insert into public.source (client_id, offer_id, client_type) values ('7', 77777, 1);


analyze public.source;
analyze public.offer;
