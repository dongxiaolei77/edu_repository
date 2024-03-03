drop table if exists edu_dm.dm_visit_consult;
create table if not exists edu_dm.dm_visit_consult
(
    -- ָ���ֶ�
    sid_visit      bigint comment '������',
    sid_consult    bigint comment '��ѯ��',
    -- ά���ֶ�
    hourinfo       string comment 'Сʱ',
    area           string comment '����ά��',
    origin_channel string comment '��Դ����ά��',
    seo_source     string comment '������Դά��',
    from_url       string comment '�ܷ�urlά��',
    --�����ǩ
    time_type      string comment 'ʱ����: 1 Сʱ  2 �� 3�� 4 ���� 5 ��',
    group_type     string comment '��Ʒ����ά�ȱ��: 1 ����  2 ��Դ���� 3 ������Դ 4 �ܷ�url 5�ܷ�����'

) partitioned by (yearinfo string, quarterinfo string, monthinfo string, dayinfo string)
    row format delimited fields terminated by '\t';