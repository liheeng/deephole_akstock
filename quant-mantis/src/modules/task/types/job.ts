// ==============================
// 枚举定义
// ==============================
export enum JobType {
    CN_DAILY_SYNC = "cn_daily_sync",
    HK_DAILY_SYNC = "hk_daily_sync",
    US_DAILY_SYNC = "us_daily_sync",
    PYTHON_SCRIPT = "python_script",
}

export enum JobConcurrencyKey {
    CN_DAILY_SYNC = "cn_daily_sync",
    HK_DAILY_SYNC = "hk_daily_sync",
    US_DAILY_SYNC = "us_daily_sync",
}

export enum JobStatus {
    CREATED = "CREATED",
    QUEUED = "QUEUED",
    RUNNING = "RUNNING",
    SUCCESS = "SUCCESS",
    FAILED = "FAILED",
}

// ==============================
// JobDefinition
// ==============================
export class JobDefinition {
    type: JobType;
    concurrency_key: string;
    max_concurrency: number;
    singleton: boolean;

    constructor(
        type: JobType,
        concurrency_key: string,
        max_concurrency: number,
        singleton = false
    ) {
        this.type = type;
        this.concurrency_key = concurrency_key;
        this.max_concurrency = max_concurrency;
        this.singleton = singleton;
    }
}

// ==============================
// 全局任务定义（对应 Python JOB_DEFINITIONS）
// ==============================
export const JOB_DEFINITIONS: Record<JobType, JobDefinition> = {
    [JobType.US_DAILY_SYNC]: new JobDefinition(
        JobType.US_DAILY_SYNC,
        JobConcurrencyKey.US_DAILY_SYNC,
        1,
        true
    ),
    [JobType.HK_DAILY_SYNC]: new JobDefinition(
        JobType.HK_DAILY_SYNC,
        JobConcurrencyKey.HK_DAILY_SYNC,
        1,
        true
    ),
    [JobType.CN_DAILY_SYNC]: new JobDefinition(
        JobType.CN_DAILY_SYNC,
        JobConcurrencyKey.CN_DAILY_SYNC,
        1,
        true
    ),
    [JobType.PYTHON_SCRIPT]: new JobDefinition(
        JobType.PYTHON_SCRIPT,
        "",
        5,
        false
    ),
};

// ==============================
// Job 实体类
// ==============================

// 解决循环引用：先声明 Task
export interface Task {
    id: string;
    status: any;
    jobs: Job[];
}

export class Job {
    id: string;
    type: JobType;
    task_id: string | null = null;
    task: Task | null = null;
    status: JobStatus = JobStatus.CREATED;
    params: Record<string, any> = {};
    depends_on: string[] = [];

    retry_count = 0;
    retries = 3;

    execute_time = "";
    stop_time = "";
    message = "";
    error = "";
    data_source = "";

    constructor(id: string, type: JobType) {
        this.id = id;
        this.type = type;
    }

    // 对应 Python get_id(self)
    getId(): string {
        return this.id;
    }

    // 对应 Python update_status
    updateStatus(newStatus: JobStatus): JobStatus {
        const oldStatus = this.status;
        this.status = newStatus;
        return oldStatus;
    }
}