/**
 * 把带千分位、字符串、空值 转换成数字
 * @param value 输入值：'7,908,518' | '5000000' | 123 | null | undefined
 * @returns 干净的数字，失败返回 0
 * 安全将带逗号的字符串/数字 转换为数字
 * 无效值 → 返回 null
 * 有效值 → 返回 number
 */
export function asNumber(value: string | number | null | undefined): number | null {
  // 空值直接返回 null
  if (value === null || value === undefined || value === '') {
    return null;
  }

  // 转字符串 → 移除所有千分位逗号
  const cleaned = String(value).replace(/,/g, '').trim();

  // 转数字
  const num = Number(cleaned);

  // 无效数字 → null，有效 → 返回数字
  return Number.isNaN(num) ? null : num;
}