FROM python:3.9-slim

# Install git
RUN apt-get update && apt-get install -y git

# Set the Git user configuration
RUN git config --global user.email "abhi555shek@gmail.com"
RUN git config --global user.name "Abhishek Gupta"

WORKDIR /app

COPY . /app

RUN pip install -r requirements.txt  # if you have dependencies

CMD ["python", "main.py"]
