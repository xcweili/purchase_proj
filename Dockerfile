# 使用debian作为基础镜像（更小，更容易获取）
FROM python:3.12-slim

# 设置工作目录
WORKDIR /app

# 安装必要的系统依赖（用于运行Python）
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

# 复制现有的.venv目录（已包含所有Python依赖）
COPY .venv .venv

# 复制项目源代码
COPY src/ src/
COPY static/ static/

# 设置环境变量
ENV PATH="/app/.venv/bin:$PATH"
ENV PYTHONPATH="/app"

# 暴露端口
EXPOSE 8000

# 启动命令
CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8000"]