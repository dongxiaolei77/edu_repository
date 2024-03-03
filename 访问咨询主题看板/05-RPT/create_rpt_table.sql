
drop table if exists edu_rpt.visit_consult;

create table if not exists edu_rpt.visit_consult
(
    sid_visit      bigint comment '������',
    sid_consult    bigint comment '��ѯ����',
    area           text comment '����ά��',
    origin_channel text comment '��Դ����ά��',
    seo_source     text comment '������Դά��',
    from_url       text comment '�ܷ�url',
    time_type      text comment 'ʱ����: 1 Сʱ  2 �� 3�� 4 ���� 5 ��',
    group_type     text comment '��Ʒ����ά�ȱ��: 1 ����  2 ��Դ���� 3 ������Դ 4 �ܷ�url 5�ܷ�����',
    yearinfo       text comment '���',
    quarterinfo    text comment '����',
    monthinfo      text comment '�·�',
    dayinfo        text comment '��',
    hourinfo       text comment 'Сʱ'
);