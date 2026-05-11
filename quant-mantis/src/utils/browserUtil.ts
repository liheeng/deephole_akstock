export const getCurrentDomain = () => {
  // 前端运行在浏览器，window 一定存在
  return window.location.origin;
};