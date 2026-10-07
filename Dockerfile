FROM apache/airflow:3.3.0

COPY --chown=airflow:root pyproject.toml uv.lock /opt/airflow/dependencies/

RUN uv export --project /opt/airflow/dependencies --frozen --no-dev --no-emit-project \
        --output-file /tmp/pipeline-requirements.txt > /dev/null \
    && pip install --quiet --no-cache-dir --require-hashes --no-deps \
        -r /tmp/pipeline-requirements.txt \
    && pip check
