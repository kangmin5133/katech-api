# 베이스 이미지를 설정합니다.
FROM python:3.11

# 작업 디렉터리를 설정합니다.
WORKDIR /workspace

# 필요한 디렉터리를 생성합니다.
RUN mkdir -p /workspace/app /workspace/config /workspace/data /workspace/logs

# requirements.txt 파일을 컨테이너에 복사합니다.
COPY requirements.txt .

# 필요한 Python 패키지를 설치합니다.
RUN pip install --no-cache-dir -r requirements.txt
RUN adduser --disabled-password --gecos "" tbelldev

# 앱을 실행합니다.
CMD ["python", "main.py"]