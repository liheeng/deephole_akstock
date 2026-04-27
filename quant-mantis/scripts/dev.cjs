const { spawn } = require('child_process');
const os = require('os');

// 自动获取本机局域网 IP（不需要任何配置！）
function getLocalIP() {
  const interfaces = os.networkInterfaces();
  for (const name of Object.keys(interfaces)) {
    for (const iface of interfaces[name]) {
      if (iface.family === 'IPv4' && !iface.internal && iface.address.startsWith('192.168.')) {
        return iface.address;
      }
    }
  }
  return '127.0.0.1';
}

// 自动设置 API 地址
const ip = getLocalIP();
process.env.VITE_API_URL = `http://${ip}:8000`;

console.log('✅ 本机IP:', ip);
console.log('✅ API 地址:', process.env.VITE_API_URL);

// 启动 vite
const vite = spawn('vite', ['--host'], { stdio: 'inherit', shell: true });
vite.on('close', (code) => process.exit(code));