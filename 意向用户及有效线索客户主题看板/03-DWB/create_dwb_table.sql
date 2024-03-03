drop table if exists edu_dwb.intention_valid_clue_dwb;
create table if not exists edu_dwb.intention_valid_clue_dwb
(
    -- ָ���ֶ�
    customer_id          string comment '�ͻ�id',
    valid_clue           string comment '�Ƿ�Ϊ��Ч���� ��ЧΪ0 ��ЧΪ1',
    -- ά���ֶ�
    new_old_state        string comment '�ж����Ͽͻ�',
    origin_type_state    string comment '�ж���������',
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
) comment '����ͻ�����Ч����������'
    partitioned by (yearinfo string, monthinfo string)
    row format delimited fields terminated by '\t'
    stored as orc
    TBLPROPERTIES ('orc.compress' = 'SNAPPY' );