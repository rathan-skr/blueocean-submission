FROM python:3.12-slim
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1
WORKDIR /app
COPY requirements.txt requirements-dev.txt ./
RUN pip install -r requirements-dev.txt
# harness/ and incumbent/ are only used by the test suite (reproducers spawn the real bank.py)
COPY settle ./settle
COPY tests ./tests
COPY harness ./harness
COPY incumbent ./incumbent
COPY pytest.ini ./
