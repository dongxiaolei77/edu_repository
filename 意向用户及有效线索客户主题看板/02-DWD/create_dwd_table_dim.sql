-- ������������С �Ҳ������޸� ����ʹ��ȫ�����ǲ��� ����Ҫ������������
-- itcast_subject��ѧ�Ʊ�(ά�ȱ�)
drop table if exists edu_dwd.dwd_itcast_subject_dim;
create table edu_dwd.dwd_itcast_subject_dim
(
    id               int,
    create_date_time string comment '����ʱ��',
    update_date_time string comment '������ʱ��',
    deleted          int comment '�Ƿ�ɾ�������ã�',
    name             string comment 'ѧ������',
    code             string,
    tenant           int

) partitioned by (start_time string)
    row format delimited fields terminated by '\t'
    stored as orc
    TBLPROPERTIES ('orc.compress' = 'SNAPPY' );

-- itcast_school У���� (ά�ȱ�)
drop table if exists edu_dwd.dwd_itcast_school_dim;
create table edu_dwd.dwd_itcast_school_dim
(
    id               int,
    create_date_time string comment '����ʱ��',
    update_date_time string comment '������ʱ��',
    deleted          int comment '�Ƿ�ɾ�������ã�',
    name             string comment 'У������',
    code             string,
    tenant           int
) partitioned by (start_time string)
    row format delimited fields terminated by '\t'
    stored as orc
    TBLPROPERTIES ('orc.compress' = 'SNAPPY' );

-- employee��Ա����(ά�ȱ�)
drop table if exists edu_dwd.dwd_employee_dim;
create table edu_dwd.dwd_employee_dim
(
    id                  int,
    email               string comment '��˾���䣬OA��¼�˺�',
    real_name           string comment 'Ա������ʵ����',
    phone               string comment '�ֻ��ţ�Ŀǰ��û��ʹ�ã���˽����OA�ӿ�û���ṩ������ԣ�',
    department_id       string comment 'OA�еĲ��ű�ţ��и�ֵ',
    department_name     string comment 'OA�еĲ�����',
    remote_login        int comment 'Ա���Ƿ����Զ�̵�¼',
    job_number          string comment 'Ա������',
    cross_school        int comment '�Ƿ��п�У��Ȩ��',
    last_login_date     string comment '����¼����',
    creator             int comment '������',
    create_date_time    string comment '����ʱ��',
    update_date_time    string comment '������ʱ��',
    deleted             int comment '�Ƿ�ɾ�������ã�',
    scrm_department_id  int comment 'SCRM�ڲ�����id',
    leave_office        int comment '��ְ״̬',
    leave_office_time   string comment '��ְʱ��',
    reinstated_time     string comment '��ְʱ��',
    superior_leaders_id int comment '�ϼ��쵼ID',
    tdepart_id          int comment 'ֱ������',
    tenant              int,
    ems_user_name       string
) partitioned by (start_time string)
    row format delimited fields terminated by '\t'
    stored as orc
    TBLPROPERTIES ('orc.compress' = 'SNAPPY' );

-- scrm_department�����ű�(ά�ȱ�)
drop table if exists edu_dwd.dwd_scrm_department_dim;
create table edu_dwd.dwd_scrm_department_dim
(
    id               int,
    name             string comment '��������',
    parent_id        int comment '������id',
    create_date_time string comment '����ʱ��',
    update_date_time string comment '����ʱ��',
    deleted          int comment 'ɾ����־',
    id_path          string comment '����ȫ·��',
    tdepart_code     int comment 'ֱ������',
    creator          string comment '������',
    depart_level     int comment '���Ų㼶',
    depart_sign      int comment '���ű�־����ʱĬ��1',
    depart_line      int comment 'ҵ���ߣ��洢ҵ���߱���',
    depart_sort      int comment '�����ֶ�',
    disable_flag     int comment '���ñ�־',
    tenant           int
) partitioned by (start_time string)
    row format delimited fields terminated by '\t'
    stored as orc
    TBLPROPERTIES ('orc.compress' = 'SNAPPY' );
