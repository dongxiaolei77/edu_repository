-- 由于数据量较小 且不经常修改 所以使用全量覆盖操作 不需要进行拉链操作
-- itcast_subject（学科表）(维度表)
drop table if exists edu_dwd.dwd_itcast_subject_dim;
create table edu_dwd.dwd_itcast_subject_dim
(
    id               int,
    create_date_time string comment '创建时间',
    update_date_time string comment '最后更新时间',
    deleted          int comment '是否被删除（禁用）',
    name             string comment '学科名称',
    code             string,
    tenant           int

) partitioned by (start_time string)
    row format delimited fields terminated by '\t'
    stored as orc
    TBLPROPERTIES ('orc.compress' = 'SNAPPY' );

-- itcast_school 校区表 (维度表)
drop table if exists edu_dwd.dwd_itcast_school_dim;
create table edu_dwd.dwd_itcast_school_dim
(
    id               int,
    create_date_time string comment '创建时间',
    update_date_time string comment '最后更新时间',
    deleted          int comment '是否被删除（禁用）',
    name             string comment '校区名称',
    code             string,
    tenant           int
) partitioned by (start_time string)
    row format delimited fields terminated by '\t'
    stored as orc
    TBLPROPERTIES ('orc.compress' = 'SNAPPY' );

-- employee（员工表）(维度表)
drop table if exists edu_dwd.dwd_employee_dim;
create table edu_dwd.dwd_employee_dim
(
    id                  int,
    email               string comment '公司邮箱，OA登录账号',
    real_name           string comment '员工的真实姓名',
    phone               string comment '手机号，目前还没有使用；隐私问题OA接口没有提供这个属性，',
    department_id       string comment 'OA中的部门编号，有负值',
    department_name     string comment 'OA中的部门名',
    remote_login        int comment '员工是否可以远程登录',
    job_number          string comment '员工工号',
    cross_school        int comment '是否有跨校区权限',
    last_login_date     string comment '最后登录日期',
    creator             int comment '创建人',
    create_date_time    string comment '创建时间',
    update_date_time    string comment '最后更新时间',
    deleted             int comment '是否被删除（禁用）',
    scrm_department_id  int comment 'SCRM内部部门id',
    leave_office        int comment '离职状态',
    leave_office_time   string comment '离职时间',
    reinstated_time     string comment '复职时间',
    superior_leaders_id int comment '上级领导ID',
    tdepart_id          int comment '直属部门',
    tenant              int,
    ems_user_name       string
) partitioned by (start_time string)
    row format delimited fields terminated by '\t'
    stored as orc
    TBLPROPERTIES ('orc.compress' = 'SNAPPY' );

-- scrm_department（部门表）(维度表)
drop table if exists edu_dwd.dwd_scrm_department_dim;
create table edu_dwd.dwd_scrm_department_dim
(
    id               int,
    name             string comment '部门名称',
    parent_id        int comment '父部门id',
    create_date_time string comment '创建时间',
    update_date_time string comment '更新时间',
    deleted          int comment '删除标志',
    id_path          string comment '编码全路径',
    tdepart_code     int comment '直属部门',
    creator          string comment '创建者',
    depart_level     int comment '部门层级',
    depart_sign      int comment '部门标志，暂时默认1',
    depart_line      int comment '业务线，存储业务线编码',
    depart_sort      int comment '排序字段',
    disable_flag     int comment '禁用标志',
    tenant           int
) partitioned by (start_time string)
    row format delimited fields terminated by '\t'
    stored as orc
    TBLPROPERTIES ('orc.compress' = 'SNAPPY' );
