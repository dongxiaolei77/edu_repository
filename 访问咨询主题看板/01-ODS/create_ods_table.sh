#! /bin/bash
HIVE_HOME=/usr/bin/hive

${HIVE_HOME} -e "
create table if not exists edu_ods.web_chat_ems
(

    id                           INT comment '����',
    create_date_time             STRING comment '���ݴ���ʱ��',
    session_id                   STRING comment '��İSESsionId',
    sid                          STRING comment '�ÿ�id',
    create_time                  STRING comment '�Ự����ʱ��',
    seo_source                   STRING comment '������Դ',
    seo_keywords                 STRING comment '�ؼ���',
    ip                           STRING comment 'IP��ַ',
    AREA                         STRING comment '����',
    country                      STRING comment '���ڹ���',
    province                     STRING comment 'ʡ',
    city                         STRING comment '����',
    origin_channel               STRING comment 'Ͷ������',
    `user`                       STRING comment '������ϯ',
    manual_time                  STRING comment '�˹���ʼʱ��',
    begin_time                   STRING comment '��ϯ��ȡʱ��',
    end_time                     STRING comment '�Ự����ʱ��',
    last_customer_msg_time_stamp STRING comment '�ͻ����һ����Ϣ��ʱ��',
    last_agent_msg_time_stamp    STRING comment '��ϯ���һ�»ظ���ʱ��',
    reply_msg_count              INT comment '�ͷ��ظ���Ϣ��',
    msg_count                    INT comment '�ͻ�������Ϣ��',
    browser_name                 STRING comment '���������',
    os_info                      STRING comment 'ϵͳ����'
)
    partitioned by (dt string)
    row format delimited fields terminated by '\t'
    stored as orc
    TBLPROPERTIES ('orc.compress' = 'SNAPPY' );


create table if not exists edu_ods.web_chat_text_ems
(
    id                   INT comment '����',
    referrer             string comment '�ϼ���Դҳ��',
    from_url             string comment '�Ự��Դҳ��',
    landing_page_url     string comment '�ÿ���½ҳ��',
    url_title            string comment '��ѯҳ��title',
    platform_description string comment '�ͻ�ƽ̨��Ϣ',
    other_params         string comment '��չ�ֶ�������',
    history              string comment '��ʷ���ʼ�¼'
)
    partitioned by (dt string)
    row format delimited fields terminated by '\t'
    stored as orc
    TBLPROPERTIES ('orc.compress' = 'SNAPPY' );"