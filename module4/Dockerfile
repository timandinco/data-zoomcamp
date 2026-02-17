FROM python:3.11-slim

# System deps
RUN apt-get update && apt-get install -y \
    curl \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Python deps
RUN pip install --no-cache-dir \
    jupyterlab \
    pandas \
    pyarrow \
    requests \
    google-cloud-storage

WORKDIR /workspace

EXPOSE 8888

CMD ["jupyter", "lab", "--ip=0.0.0.0", "--no-browser", "--allow-root"]
