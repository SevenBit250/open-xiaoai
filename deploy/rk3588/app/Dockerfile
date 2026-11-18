FROM python:3.12-slim

# 清华 PyPI 源加速国内构建
RUN pip install --no-cache-dir \
    -i https://pypi.tuna.tsinghua.edu.cn/simple \
    sherpa-onnx==2.0.1 \
    numpy \
    websockets

WORKDIR /app
COPY server.py .

EXPOSE 4399
CMD ["python", "server.py"]
