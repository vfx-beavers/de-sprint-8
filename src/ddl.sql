CREATE TABLE IF NOT EXISTS public.subscribers_feedback (
  id serial4 NOT NULL,
    restaurant_id varchar(50) NOT NULL,
    adv_campaign_id varchar(50) NOT NULL,
    adv_campaign_content text NOT NULL,
    adv_campaign_owner varchar(100) NOT NULL,
    adv_campaign_owner_contact varchar(60) NOT NULL,
    adv_campaign_datetime_start int8 NOT NULL,
    adv_campaign_datetime_end int8 NOT NULL,
    datetime_created int8 NOT NULL,
    client_id varchar(50) NOT NULL,
    trigger_datetime_created int4 NOT NULL,
    feedback varchar NULL,
    CONSTRAINT id_pk PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS public.subscribers_restaurants (
    id serial4 NOT NULL,
    client_id varchar NOT NULL,
    restaurant_id varchar NOT NULL,
    CONSTRAINT pk_id PRIMARY KEY (id)
);