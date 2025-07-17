FROM --platform=linux/amd64 python:3.10-slim

WORKDIR /app


# Install necessary build tools and system libraries
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    git \
    libreoffice \
    firefox-esr \
    wget \
    unzip \
    libcairo2-dev \
    python3-dev \
    libgomp1 \
    libomp-dev \
    && \
    apt-get clean

# Install Chrome
RUN wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb && \
    apt install -y ./google-chrome-stable_current_amd64.deb && \
    rm google-chrome-stable_current_amd64.deb

# Install Python dependencies, allowing libsvm to install correctly
COPY data_extraction/requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Set display port for headless browser
ENV DISPLAY=:99

COPY . .

RUN cp /app/data_extraction/routes.py /app/routes.py
EXPOSE 5001

CMD ["python", "/app/routes.py"]