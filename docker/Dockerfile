# ---------- frontend build ----------
FROM node:20 AS frontend-builder

WORKDIR /frontend

COPY quant-mantis/package.json quant-mantis/package-lock.json ./
RUN npm install --registry=https://mirrors.cloud.tencent.com/npm/

COPY quant-mantis/ .

RUN npm run build


# ---------- backend + runtime ----------
FROM python:3.10-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    nginx curl \
    && rm -rf /var/lib/apt/lists/*

# supercronic
RUN curl -L https://github.com/aptible/supercronic/releases/latest/download/supercronic-linux-amd64 \
    -o /usr/local/bin/supercronic && \
    chmod +x /usr/local/bin/supercronic

COPY requirements.txt .
# RUN pip install --no-cache-dir -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple --trusted-host pypi.tuna.tsinghua.edu.cn
RUN pip install -r requirements.txt -i https://mirrors.aliyun.com/pypi/simple/ --trusted-host mirrors.aliyun.com

COPY app/ /app/

# ✅ 把 Vite build 结果放进 nginx
COPY --from=frontend-builder /frontend/dist /usr/share/nginx/html

COPY docker/nginx.conf /etc/nginx/nginx.conf

RUN mkdir -p /data /logs /notebooks_runtime

COPY docker/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

CMD ["/entrypoint.sh"]