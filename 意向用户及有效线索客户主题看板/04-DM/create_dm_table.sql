drop table if exists edu_dm.intention_valid_clue_dm;
create table if not exists edu_dm.intention_valid_clue_dm
(
    -- ָ���ֶ�
    count_intention      bigint comment '��������',
    count_valid_clue     bigint comment '��Ч������',
    -- �����ֶ�
    time_type            string comment '�ж�ʱ��ά�� 1ΪСʱ 2Ϊ�� 3Ϊ�� 4Ϊ��',
    group_type           string comment '�ж�����ά�� 1Ϊ���� 2Ϊѧ�� 3ΪУ�� 4Ϊ��Դ���� 5Ϊ��ѯ����',
    -- ά���ֶ�
    new_old_state        string comment '�ж����Ͽͻ� �漰����ά�Ⱦ���ֵ ���漰 ����ά�Ⱦ�Ϊ -1',
    origin_type_state    string comment '�ж��������� �漰�������¾���֮ ���漰 ��������ά�Ⱦ�Ϊ -1',
    area                 string comment '����',
    itcast_subject_id    string comment 'ѧ��id',
    itcast_subject_name  string comment 'ѧ����',
    itcast_school_id     string comment 'У��id',
    itcast_school_name   string comment 'У����',
    origin_type          string comment '��Դ����',
    scrm_department_id   string comment '��ѯ����id',
    scrm_department_name string comment '��ѯ��������',
    hourinfo             string comment 'Сʱ',
    dayinfo              string comment '��'
) comment '����ͻ�����Ч��������'
    partitioned by (yearinfo string, monthinfo string)
    row format delimited fields terminated by '\t';