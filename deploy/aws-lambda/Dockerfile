# Container image for AWS Lambda. Build from the repository root:
#   docker build -f deploy/aws-lambda/Dockerfile -t brazil-ingest-lambda .
FROM public.ecr.aws/lambda/python:3.12

WORKDIR /build
COPY pyproject.toml README.md LICENSE ./
COPY src ./src
RUN pip install --no-cache-dir --target "${LAMBDA_TASK_ROOT}" ".[s3]" && rm -rf /build

WORKDIR ${LAMBDA_TASK_ROOT}
CMD ["brazil_ingest.aws_lambda.handler"]
