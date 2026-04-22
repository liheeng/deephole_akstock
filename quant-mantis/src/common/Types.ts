export interface CheckResult {
    errors: string[]      // 错误信息列表
    isValid: () => boolean
}

export class SimpleCheckResult implements CheckResult {
    errors: string[]

    constructor(...errors: string[]) {
        this.errors = errors
    }

    isValid() {
        return this.errors.length === 0
    }
}