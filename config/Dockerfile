FROM python:3.7-slim

RUN apt-get update && apt-get install -y netcat && apt-get install dos2unix

# User arguments on Dockerfile build
ARG SETTINGS
ARG PORT

# Set working directory
WORKDIR /opt/visualisation_engine

# Install Project requirements
COPY requirements.txt .
RUN pip3 install -r requirements.txt

COPY . .

# Copy .env file to settings
COPY config/.env visualisation_engine/settings
COPY config/.env visualisation_engine/

EXPOSE ${PORT}
RUN dos2unix config/run.sh
CMD bash config/run.sh
