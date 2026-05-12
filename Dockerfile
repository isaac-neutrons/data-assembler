FROM python:3.12

WORKDIR /app

COPY pyproject.toml .
COPY src/ ./src/
COPY tests/ ./tests/
COPY README.md .

RUN pip install --no-cache-dir -e .

RUN pip install --no-cache-dir --no-deps git+https://github.com/isaac-neutrons/nr-isaac-format.git \
 && pip install --no-cache-dir 'jsonschema>=4.0' 'python-ulid>=2.0' 'click>=8.0' 'pyyaml>=6.0' 'httpx>=0.27' 'python-dotenv>=1.0'

RUN chmod og+rwX -R /app

ENTRYPOINT ["/bin/bash"]