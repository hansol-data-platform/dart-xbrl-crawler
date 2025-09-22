# AWS Lambda용 Python 컨테이너 이미지
FROM public.ecr.aws/lambda/python:3.9

# 시스템 의존성 설치 (pyarrow 빌드를 위한 도구들 포함)
RUN yum update -y && \
    yum install -y \
        gcc \
        gcc-c++ \
        cmake3 \
        make \
        && \
    ln -s /usr/bin/cmake3 /usr/bin/cmake && \
    yum clean all

# Python 패키지 설치
COPY requirements_lambda_py313.txt ${LAMBDA_TASK_ROOT}/requirements.txt

# pip 업그레이드 및 패키지 설치 (pyarrow는 바이너리 우선)
RUN pip install --upgrade pip && \
    pip install --prefer-binary --no-cache-dir -r requirements.txt

# Lambda 함수 코드 복사
COPY lambda_function.py ${LAMBDA_TASK_ROOT}
COPY dart_api_manager.py ${LAMBDA_TASK_ROOT}
COPY s3_uploader.py ${LAMBDA_TASK_ROOT}
COPY xbrl_processor.py ${LAMBDA_TASK_ROOT}
COPY xbrl_batch_processor.py ${LAMBDA_TASK_ROOT}
COPY corp_list.json ${LAMBDA_TASK_ROOT}

# Lambda 핸들러 설정
CMD ["lambda_function.lambda_handler"]