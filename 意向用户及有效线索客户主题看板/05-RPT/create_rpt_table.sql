# ��Щ������mysql��ִ��
drop table if exists edu_rpt.intention_valid_clue;
create table if not exists edu_rpt.intention_valid_clue
(
    -- ָ���ֶ�
    count_intention      bigint comment '��������',
    count_valid_clue     bigint comment '��Ч������',
    -- �����ֶ�
    time_type            varchar(500) comment '�ж�ʱ��ά�� 1ΪСʱ 2Ϊ�� 3Ϊ�� 4Ϊ��',
    group_type           varchar(500) comment '�ж�����ά�� 1Ϊ���� 2Ϊѧ�� 3ΪУ�� 4Ϊ��Դ���� 5Ϊ��ѯ����',
    -- ά���ֶ�
    new_old_state        varchar(500) comment '�ж����Ͽͻ� �漰����ά�Ⱦ���ֵ ���漰 ����ά�Ⱦ�Ϊ -1',
    origin_type_state    varchar(500) comment '�ж��������� �漰�������¾���֮ ���漰 ��������ά�Ⱦ�Ϊ -1',
    area                 varchar(500) comment '����',
    itcast_subject_id    varchar(500) comment 'ѧ��id',
    itcast_subject_name  varchar(500) comment 'ѧ����',
    itcast_school_id     varchar(500) comment 'У��id',
    itcast_school_name   varchar(500) comment 'У����',
    origin_type          varchar(500) comment '��Դ����',
    scrm_department_id   varchar(500) comment '��ѯ����id',
    scrm_department_name varchar(500) comment '��ѯ��������',
    hourinfo             varchar(500) comment 'Сʱ',
    dayinfo              varchar(500) comment '��',
    yearinfo              varchar(500) comment '���',
    monthinfo              varchar(500) comment '�·�'
) comment '����ͻ�����Ч��������'
;
