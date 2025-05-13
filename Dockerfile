FROM python:3.11-slim-buster

RUN apt-get update \
 && apt-get update --fix-missing \
 && apt-get install -f \
 && apt-get install -y git \
 && apt-get install -y build-essential \
 && apt-get install -y nano \
 && apt-get install -y tree

# Set working directory
WORKDIR /app

# Set environment variables for Bitbucket credentials
ENV BITBUCKET_ACCESS_TOKEN=

# Clone the Bitbucket repo and install setup.py packages
RUN git config --global credential.helper 'store --file /tmp/git-credentials' \
  && echo "https://x-token-auth:$BITBUCKET_ACCESS_TOKEN@bitbucket.org" > /tmp/git-credentials \
  && git clone --depth 1 https://bitbucket.org/thepredictivecompany/data_api.git
  #&& git clone -b development https://bitbucket.org/thepredictivecompany/data_api.git

# Set working directory
WORKDIR /app/data_api

# Run the create_folder_structure.sh script
RUN chmod +x create_folder_structure.sh && ./create_folder_structure.sh

RUN pip install --upgrade pip \
  && pip install poetry

RUN poetry cache clear --all . \
  && poetry install --no-root

ENV FLASK_APP=app.py
ENV FLASK_RUN_HOST=0.0.0.0
ENV FLASK_RUN_PORT=5000
ENV FLASK_DEBUG=1
ENV PYTHONUNBUFFERED=1
EXPOSE 5000

CMD ["poetry", "run", "flask", "run"]