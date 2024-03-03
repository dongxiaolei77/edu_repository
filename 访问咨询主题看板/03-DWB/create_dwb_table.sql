drop table if exists edu_dwb.visit_consult_dwb;
create table if not exists edu_dwb.visit_consult_dwb
(
    -- ������ѯ�����ֶ� DWD��
    id                           int comment '����',
    create_date_time             string comment '���ݴ���ʱ��',
    session_id                   string comment '��İSESsionId',
    sid                          string comment '�ÿ�id',
    create_time                  string comment '�Ự����ʱ��',
    seo_source                   string comment '������Դ',
    seo_keywords                 string comment '�ؼ���',
    ip                           string comment 'IP��ַ',
    AREA                         string comment '����',
    country                      string comment '���ڹ���',
    province                     string comment 'ʡ',
    city                         string comment '����',
    origin_channel               string comment 'Ͷ������',
    `user`                       string comment '������ϯ',
    manual_time                  string comment '�˹���ʼʱ��',
    begin_time                   string comment '��ϯ��ȡʱ��',
    end_time                     string comment '�Ự����ʱ��',
    last_customer_msg_time_stamp string comment '�ͻ����һ����Ϣ��ʱ��',
    last_agent_msg_time_stamp    string comment '��ϯ���һ�»ظ���ʱ��',
    reply_msg_count              int comment '�ͷ��ظ���Ϣ��',
    msg_count                    int comment '�ͻ�������Ϣ��',
    browser_name                 string comment '���������',
    os_info                      string comment 'ϵͳ����',
    hourinfo                     string comment '���ݴ���ʱ��-Сʱ',

    -- ������ѯ��¼������ �����ֶ� DWD��
    referrer                     string comment '�ϼ���Դҳ��',
    from_url                     string comment '�Ự��Դҳ��',
    landing_page_url             string comment '�ÿ���½ҳ��',
    url_title                    string comment '��ѯҳ��title',
    platform_description         string comment '�ͻ�ƽ̨��Ϣ',
    other_params                 string comment '��չ�ֶ�������',
    history                      string comment '��ʷ���ʼ�¼'


) partitioned by (yearinfo string, quarterinfo string, monthinfo string, dayinfo string)
    row format delimited fields terminated by '\t'
    stored as orc
    TBLPROPERTIES ('orc.compress' = 'SNAPPY' );