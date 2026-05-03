import { Job, JobStatus } from './job';

export enum TaskStatus {
    CREATED = "CREATED",
    SUBMITTED = "SUBMITTED",
    RUNNING = "RUNNING",
    SUSPENDED = "SUSPENDED",
    SUCCESS = "SUCCESS",
    FAILED = "FAILED",
    PARTIAL_SUCCESS = "PARTIAL_SUCCESS",
}

export enum TaskMode {
    SEQUENTIAL = "sequential",
    PARALLEL = "parallel",
    DAG = "dag",
}

export class Task {
    id: string;
    description = "";
    status: TaskStatus = TaskStatus.CREATED;
    jobs: Job[] = [];
    mode: TaskMode = TaskMode.DAG;

    create_time: string;
    start_time = "";
    execute_time = "";
    stop_time = "";
    message = "";

    constructor(id: string) {
        this.id = id;
        this.create_time = new Date().toISOString().slice(0, 19).replace("T", " ");
    }

    updateStatusBasedOnJobs(): boolean {
        const oldStatus = this.status;
        const statuses = this.jobs.map((j) => j.status);

        if (statuses.every((s) => s === JobStatus.SUCCESS)) {
            this.status = TaskStatus.SUCCESS;
        } else if (statuses.some((s) => s === JobStatus.RUNNING)) {
            this.status = TaskStatus.RUNNING;
        } else if (statuses.some((s) => s === JobStatus.FAILED)) {
            const hasSuccess = statuses.some((s) => s === JobStatus.SUCCESS);
            this.status = hasSuccess ? TaskStatus.PARTIAL_SUCCESS : TaskStatus.FAILED;
        }

        return this.status !== oldStatus;
    }
}